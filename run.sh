#!/usr/bin/env bash
# usage: ./run.sh restart|ddl|ddl-caught-up production.json|recommended.json [key=value ...]
#   key=value pairs override single keys of the config file, e.g. snapshot.mode=no_data
#   DDL="..." replaces the DDL; COL becomes a column name unique to the run
set -euo pipefail
cd "$(dirname "$0")"

usage="usage: ./run.sh restart|ddl|ddl-caught-up production.json|recommended.json [key=value ...]"
scenario=${1:?$usage}
config=${2:?$usage}
shift 2
run=r$(date +%H%M%S)   # tags the rows and names the connector and its topics
api=http://localhost:8083
ddl=${DDL:-ALTER TABLE APP.EVENTS ADD COL VARCHAR2(40 CHAR)}
ddl=${ddl//COL/X_$run}

sql() { docker compose exec -T oracle sqlplus -s app/app@localhost:1521/FREEPDB1; }

# insert <phase> <rows> [seconds between rows], one commit per row.
# The 8 KB CLOB starts with the row's name, so Kafka can show whether each row got its own CLOB.
insert() {
  sql >/dev/null <<SQL
DECLARE v CLOB;
BEGIN
  FOR i IN 1 .. $2 LOOP
    v := RPAD('$run:$1:' || i || ':', 8000, 'x');
    INSERT INTO APP.EVENTS (EV_SITE, EV_ID, EV_NAME, EV_DATA)
    VALUES ('A', APP.EVENTS_SEQ.NEXTVAL, '$run:$1:' || i, v);
    COMMIT;
    DBMS_SESSION.SLEEP(${3:-0});
  END LOOP;
END;
/
SQL
}

# The DDL waits up to 30 s for the table lock, as ongoing inserts hold it briefly; without the
# timeout it fails with ORA-00054. Any error aborts the run.
run_ddl() {
  local out
  out=$(sql <<< "WHENEVER SQLERROR EXIT FAILURE
ALTER SESSION SET DDL_LOCK_TIMEOUT = 30;
$ddl;") || { echo "$out" >&2; exit 1; }
}

in_db() {
  sql <<< "SET HEADING OFF FEEDBACK OFF
SELECT COUNT(*) FROM APP.EVENTS WHERE EV_NAME LIKE '$run:$1:%';" | tr -d '[:space:]'
}

# one line per insert event in Kafka: "<row name> ok|bad", ok if the CLOB belongs to the row
kafka_rows() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$run.APP.EVENTS" --from-beginning --timeout-ms 10000 2>/dev/null \
    | jq -r 'select(.op == "c") | .after | .EV_NAME as $n
             | "\($n) \(if (.EV_DATA // "" | startswith($n + ":")) then "ok" else "bad" end)"'
}

# count <phase> [ok] from kafka_rows output on stdin: rows in Kafka, or rows with a correct CLOB
count() { grep "^$run:$1:[0-9]* ${2:-}" | cut -d' ' -f1 | sort -u | wc -l; }

in_kafka() { kafka_rows | count "$1"; }

step() { echo "$(date +%T)  $*"; }

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
started() { [ "$(state)" = RUNNING ] || [ "$(state)" = FAILED ]; }
offset_stored() { curl -s "$api/connectors/$run/offsets" | jq -e '.offsets | length > 0'; }
# the task reports RUNNING before its startup snapshot is done; streaming starts after it
streaming() { docker compose logs --since "$since" connect | grep -q 'Starting streaming'; }
all_delivered() { [ "$(in_kafka "$1")" -eq "$(in_db "$1")" ]; }

start_connector() {
  step "waiting for Kafka Connect"
  wait_for 300 api_up
  step "starting connector $run"
  local t0; t0=$(date -u +%FT%TZ)
  for old in $(curl -s "$api/connectors" | jq -r '.[]'); do curl -s -X DELETE "$api/connectors/$old"; done
  jq -n --args '$ARGS.positional | map(split("=") | {(.[0]): (.[1:] | join("="))}) | add // {}' "$@" \
    | jq --arg run "$run" --slurpfile base "$config" \
        '{name: $run, config: ($base[0] + {"topic.prefix": $run, "schema.history.internal.kafka.topic": "\($run).history"} + .)}' \
    | curl -s --fail-with-body -X POST -H 'Content-Type: application/json' --data @- "$api/connectors" >/tmp/connector-response.json \
    || { echo "connector rejected: $(jq -r .message /tmp/connector-response.json)" >&2; exit 1; }
  wait_for 120 started
  if [ "$(state)" = FAILED ]; then
    # e.g. an invalid combination of settings; Debezium explains it in its first ERROR line
    echo "task FAILED: $(docker compose logs --since "$t0" connect | grep -m1 ' ERROR ' | sed -E 's/.*ERROR +[^ ]+ +//')" >&2
    exit 1
  fi
}

# restart a FAILED task up to 3 times, as an external auto-restart would
auto_restart() {
  for i in 1 2 3; do
    step "checking task in 20 s ($i/3)"
    sleep 20
    running && { step "task RUNNING"; continue; }
    step "task FAILED: $(curl -s "$api/connectors/$run/status" | jq -r '.tasks[0].trace' | grep -m1 -o 'Oracle LogMiner is unable.*')"
    curl -sf -X POST "$api/connectors/$run/tasks/0/restart"
    step "task restarted"
  done
}

# The offset stored before the disruption, and where the last task start continued (from the
# Debezium log). If they differ, everything in between is skipped.
stored_scn() { curl -s "$api/connectors/$run/offsets" | jq -r '.offsets[0].offset.scn'; }
resume_point() {
  local resumed
  resumed=$(docker compose logs --since "$since" connect 2>/dev/null \
    | grep -o -E 'Snapshot ended with SnapshotResult \[status=[A-Z]+, offset=OracleOffsetContext \[scn=[0-9]+' \
    | tail -1 | grep -o -E '[0-9]+$' || true)
  echo "stored offset   scn=$stored"
  echo "resumed from    scn=${resumed:-(no restart)}"
}

report() {
  step "counting rows in the database and in Kafka"
  echo
  local rows; rows=$(kafka_rows)
  echo "task: $(state)"
  printf '%-8s %6s %6s %8s\n' phase db kafka clob-ok
  for phase in "$@"; do
    printf '%-8s %6s %6s %8s\n' "$phase" "$(in_db "$phase")" "$(count "$phase" <<< "$rows")" "$(count "$phase" ok <<< "$rows")"
  done
}

echo "== $scenario $config ${*:-} ($run)"
[ "$scenario" = restart ] || echo "DDL: $ddl"
start_connector "$@"
step "inserting 50 rows (before)"
insert before 50
step "waiting until they are in Kafka and the offset is stored"
wait_for 120 all_delivered before
wait_for 60 offset_stored
sleep 6   # one more offset flush, so the stored offset covers all "before" rows
since=$(date -u +%FT%TZ)
stored=$(stored_scn)

case "$scenario" in
  restart)
    step "stopping Kafka Connect"
    docker compose stop connect >/dev/null 2>&1
    step "inserting 50 rows while it is down (down)"
    insert down 50
    step "starting Kafka Connect, waiting for streaming"
    docker compose start connect >/dev/null 2>&1
    wait_for 300 streaming
    step "inserting 50 rows (after), waiting 20 s"
    insert after 50
    sleep 20
    resume_point
    report before down after
    ;;

  ddl)
    # rows keep coming while the DDL runs, so the connector is a few seconds behind
    step "inserting 100 rows over 10 s (during)"
    insert during 100 0.1 &
    sleep 5
    step "running the DDL"
    run_ddl
    wait
    auto_restart
    step "inserting 50 rows (after), waiting 20 s"
    insert after 50
    sleep 20
    resume_point
    report before during after
    ;;

  ddl-caught-up)
    step "running the DDL"
    run_ddl
    step "inserting 50 rows (after), waiting 20 s"
    insert after 50
    sleep 20
    report before after
    ;;

  *) echo "unknown scenario: $scenario" >&2; exit 1 ;;
esac
