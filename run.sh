#!/usr/bin/env bash
# usage: ./run.sh restart|ddl|ddl-caught-up [key=value ...]
#   key=value pairs override connector.json, e.g. snapshot.mode=no_data
set -euo pipefail
cd "$(dirname "$0")"

scenario=${1:?usage: ./run.sh restart|ddl|ddl-caught-up [key=value ...]}
shift
run=r$(date +%H%M%S)   # tags the rows and names the connector and its topics
api=http://localhost:8083

sql() { docker compose exec -T oracle sqlplus -s app/app@localhost:1521/FREEPDB1; }

# insert <phase> <rows> [seconds between rows], one commit per row
insert() {
  sql >/dev/null <<SQL
DECLARE v CLOB := RPAD('x', 8000, 'x');
BEGIN
  FOR i IN 1 .. $2 LOOP
    INSERT INTO APP.EVENTS (EV_SITE, EV_ID, EV_NAME, EV_DATA)
    VALUES ('A', APP.EVENTS_SEQ.NEXTVAL, '$run:$1:' || i, v);
    COMMIT;
    DBMS_SESSION.SLEEP(${3:-0});
  END LOOP;
END;
/
SQL
}

in_db() {
  sql <<< "SET HEADING OFF FEEDBACK OFF
SELECT COUNT(*) FROM APP.EVENTS WHERE EV_NAME LIKE '$run:$1:%';" | tr -d '[:space:]'
}

in_kafka() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$run.APP.EVENTS" --from-beginning --timeout-ms 10000 2>/dev/null \
    | jq -r 'select(.op == "c") | .after.EV_NAME' | grep "^$run:$1:" | sort -u | wc -l
}

state() { curl -s "$api/connectors/$run/status" | jq -r '.tasks[0].state // "STARTING"'; }

# wait_for <seconds> <command...>: poll until the command succeeds
wait_for() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -lt $deadline ] || { echo "timeout: $*" >&2; exit 1; }
    sleep 2
  done
}

api_up() { curl -sf "$api/connectors"; }
running() { [ "$(state)" = RUNNING ]; }
offset_stored() { curl -s "$api/connectors/$run/offsets" | jq -e '.offsets | length > 0'; }
# the task reports RUNNING before its startup snapshot is done; streaming starts after it
streaming() { docker compose logs --since "$since" connect | grep -q 'Starting streaming'; }
all_delivered() { [ "$(in_kafka "$1")" -eq "$(in_db "$1")" ]; }

start_connector() {
  wait_for 300 api_up
  for old in $(curl -s "$api/connectors" | jq -r '.[]'); do curl -s -X DELETE "$api/connectors/$old"; done
  jq -n --args '$ARGS.positional | map(split("=") | {(.[0]): (.[1:] | join("="))}) | add // {}' "$@" \
    | jq --arg run "$run" --slurpfile base connector.json \
        '{name: $run, config: ($base[0] + {"topic.prefix": $run, "schema.history.internal.kafka.topic": "\($run).history"} + .)}' \
    | curl -sf -X POST -H 'Content-Type: application/json' --data @- "$api/connectors" >/dev/null
  wait_for 120 running
}

# restart a FAILED task up to 3 times, as an external auto-restart would
auto_restart() {
  for _ in 1 2 3; do
    sleep 20
    running && continue
    echo "task FAILED: $(curl -s "$api/connectors/$run/status" | jq -r '.tasks[0].trace' | grep -m1 -o 'Oracle LogMiner is unable.*')"
    curl -sf -X POST "$api/connectors/$run/tasks/0/restart"
    echo "task restarted"
  done
}

# where the last task start resumed, from the Debezium log
resume_point() {
  docker compose logs --since "$1" connect 2>/dev/null | grep -o -E \
    'Found previous offset OracleOffsetContext \[scn=[0-9]+|Snapshot ended with SnapshotResult \[status=[A-Z]+, offset=OracleOffsetContext \[scn=[0-9]+' \
    | tail -2 | sed -E \
      -e 's/Found previous offset OracleOffsetContext \[scn=/stored offset   scn=/' \
      -e 's/Snapshot ended with SnapshotResult \[status=([A-Z]+), offset=OracleOffsetContext \[scn=/resumed from    scn=/'
}

report() {
  echo "task: $(state)"
  printf '%-8s %6s %6s\n' phase db kafka
  for phase in "$@"; do printf '%-8s %6s %6s\n' "$phase" "$(in_db "$phase")" "$(in_kafka "$phase")"; done
}

echo "== $scenario ($run) overrides: ${*:-none}"
start_connector "$@"
insert before 50
wait_for 120 all_delivered before
wait_for 60 offset_stored
sleep 6   # one more offset flush, so the stored offset covers all "before" rows
since=$(date -u +%FT%TZ)

case "$scenario" in
  restart)
    docker compose stop connect >/dev/null 2>&1
    insert down 50
    docker compose start connect >/dev/null 2>&1
    wait_for 300 streaming
    insert after 50
    sleep 20
    resume_point "$since"
    report before down after
    ;;

  ddl)
    # rows keep coming while the DDL runs, so the connector is a few seconds behind
    insert during 100 0.1 &
    sleep 5
    sql >/dev/null <<< "ALTER TABLE APP.EVENTS ADD X_$run VARCHAR2(40 CHAR);"
    wait
    auto_restart
    insert after 50
    sleep 20
    resume_point "$since"
    report before during after
    ;;

  ddl-caught-up)
    sql >/dev/null <<< "ALTER TABLE APP.EVENTS ADD X_$run VARCHAR2(40 CHAR);"
    insert after 50
    sleep 20
    report before after
    ;;

  *) echo "unknown scenario: $scenario" >&2; exit 1 ;;
esac
