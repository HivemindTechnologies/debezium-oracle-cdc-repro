#!/usr/bin/env bash
# usage: ./run.sh <scenario> 0-production.json [layer.json ...] [key=value ...]
#   scenarios: restart, ddl, ddl-caught-up, changes, archive-gone, reorg
#   REORG_SECONDS=... sets how long the reorg transaction stays open (default 300)
#   layer files are merged onto 0-production.json from left to right, e.g. 1-no-data.json;
#   key=value pairs override single keys last, e.g. lob.enabled=true
#   DDL="..." replaces the DDL; COL becomes a column name unique to the run
set -euo pipefail
cd "$(dirname "$0")"

usage="usage: ./run.sh restart|ddl|ddl-caught-up|changes|archive-gone|reorg 0-production.json [layer.json ...] [key=value ...]"
scenario=${1:?$usage}
shift
configs=()
while [ $# -gt 0 ] && [[ $1 == *.json ]]; do configs+=("$1"); shift; done
[ ${#configs[@]} -gt 0 ] || { echo "$usage" >&2; exit 1; }
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

# change <phase>: per row, one update with a new CLOB (name and CLOB get the suffix :v2), then
# one update that only touches EV_NOTE and leaves the CLOB alone
change() {
  sql >/dev/null <<SQL
DECLARE v CLOB;
BEGIN
  FOR r IN (SELECT EV_ID id, EV_NAME n FROM APP.EVENTS WHERE EV_NAME LIKE '$run:$1:%') LOOP
    v := RPAD(r.n || ':v2:', 8000, 'x');
    UPDATE APP.EVENTS SET EV_NAME = r.n || ':v2', EV_DATA = v WHERE EV_SITE = 'A' AND EV_ID = r.id;
    COMMIT;
    UPDATE APP.EVENTS SET EV_NOTE = 'note' WHERE EV_SITE = 'A' AND EV_ID = r.id;
    COMMIT;
  END LOOP;
END;
/
SQL
}

# reorg <seconds>: one transaction in the uncaptured schema REORG that inserts and deletes 200
# rows of 2 KB in a loop and commits only at the end, like a mass delete of a reorg job. About
# 5 MB of redo per second; the connector filters all of it out, except the transaction's start.
reorg() {
  docker compose exec -T oracle sqlplus -s reorg/reorg@localhost:1521/FREEPDB1 >/dev/null <<SQL
DECLARE t0 NUMBER := DBMS_UTILITY.GET_TIME;
BEGIN
  WHILE DBMS_UTILITY.GET_TIME - t0 < $1 * 100 LOOP
    INSERT INTO REORG.JUNK SELECT LEVEL, RPAD('x', 2000, 'x') FROM DUAL CONNECT BY LEVEL <= 200;
    DELETE FROM REORG.JUNK;
    DBMS_SESSION.SLEEP(0.2);
  END LOOP;
  COMMIT;
END;
/
SQL
}

redo_mb() {
  docker compose exec -T oracle sqlplus -s / as sysdba <<< "SET HEADING OFF FEEDBACK OFF
SELECT ROUND(value/1024/1024) FROM v\$sysstat WHERE name = 'redo size';" | tr -d '[:space:]'
}

# Per 30 rows of <phase> (inserted 1 per second): the largest delay from the commit in Oracle
# (source.ts_ms) to the event leaving Debezium (ts_ms).
lag_report() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$run.APP.EVENTS" --from-beginning --timeout-ms 10000 2>/dev/null \
    | jq -r --arg p "$run:$1:" 'select(.op == "c" and (.after.EV_NAME | startswith($p)))
             | "\(.after.EV_NAME | split(":")[2]) \((.ts_ms - .source.ts_ms) / 1000 | floor)"' \
    | sort -n | awk '{ b = int(($1 - 1) / 30); if ($2 > max[b]) max[b] = $2; n[b]++ }
        END { for (i = 0; i in n; i++) printf "  rows %3d-%3d   max lag %4d s\n", i*30+1, i*30+n[i], max[i] }'
}

# The LogMiner sessions since $since, from Debezium's DEBUG log: how many there were, how many
# distinct start SCNs they had, and how far the last one reached back from its end.
session_report() {
  docker compose logs --since "$since" connect 2>/dev/null \
    | sed -n -E 's/.*Starting mining session \[startScn=([0-9]+), endScn=([0-9]+).*/\1 \2/p' \
    | awk '{ n++; if (!($1 in seen)) { seen[$1]; d++ } w = $2 - $1; if (w > maxw) maxw = w }
        END { printf "mining sessions %d, distinct start SCNs %d, widest session %d SCNs\n", n, d, maxw }'
}

# the row numbers of <phase> that are in the database but not in Kafka
missing() {
  comm -23 <(seq 1 "$(in_db "$1")" | sort) <(kafka_rows | grep -o "^$run:$1:[0-9]*" | cut -d: -f3 | sort -u) \
    | sort -n | tr '\n' ' '
}

delete_rows() { sql >/dev/null <<< "DELETE FROM APP.EVENTS WHERE EV_NAME LIKE '$run:$1:%';
COMMIT;"; }

# Switch logs until the online redo holds nothing older, then delete every archive log, as a
# deletion policy under disk pressure would.
drop_archives() {
  docker compose exec -T oracle sqlplus -s / as sysdba >/dev/null <<< "ALTER SYSTEM ARCHIVE LOG CURRENT;
ALTER SYSTEM ARCHIVE LOG CURRENT;
ALTER SYSTEM ARCHIVE LOG CURRENT;"
  docker compose exec -T oracle bash -lc 'rman target / <<< "DELETE NOPROMPT ARCHIVELOG ALL;"' >/dev/null
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
    | jq -r 'select(.op == "c" or .op == "r") | .after | .EV_NAME as $n
             | "\($n) \(if (.EV_DATA // "" | startswith($n + ":")) then "ok" else "bad" end)"'
}

# one line per event, in Kafka order: "<phase> <event> <clob> <row>", clob is the version the
# event carried (v1, v2, empty, other), row is the row's name without the :v2 suffix
kafka_events() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$run.APP.EVENTS" --from-beginning --timeout-ms 10000 2>/dev/null \
    | jq -r 'select(. != null and .op != "r")
             | if .op == "d" then "\(.before.EV_NAME | split(":")[1]) delete - \(.before.EV_NAME | sub(":v2$"; ""))"
               else .after as $a
                 | (if .op == "c" then "insert" elif $a.EV_NOTE == null then "update-clob" else "update-note" end) as $e
                 | ($a.EV_DATA // "") as $d
                 | ($a.EV_NAME | sub(":v2$"; "")) as $row
                 | (if ($d | startswith($row + ":x")) then "v1" elif ($d | startswith($row + ":v2:x")) then "v2"
                    elif $d == "" then "empty" else "other" end) as $c
                 | "\($a.EV_NAME | split(":")[1]) \($e) \($c) \($row)"
               end'
}

# For each group of 50 rows: every change made in the table, next to what reached Kafka for it,
# then the end state. A target is correct if the last event per row matches the table.
change_report() {
  step "counting events in Kafka"
  local events last
  events=$(kafka_events)
  last=$(awk '{ last[$4] = $1 " " ($2 == "delete" ? "delete" : $3) } END { for (r in last) print last[r] }' <<< "$events")
  echo
  echo "task: $(state)"
  [ "$(state)" = FAILED ] && echo "      $(reason)"
  for phase in kept deleted; do
    echo
    echo "$phase (50 rows)"
    printf '  %-34s     %s\n' "in the table" "in Kafka"
    change_line "$phase" insert      v1 "insert, CLOB v1"
    change_line "$phase" update-clob v2 "update, CLOB v2"
    change_line "$phase" update-note v2 "update, other column"
    if [ "$phase" = kept ]; then
      end_line kept v2 "end: $(in_db kept) rows, CLOB v2"
    else
      change_line deleted delete - "delete"
      end_line deleted delete "end: $(in_db deleted) rows"
    fi
  done
}

# change_line <phase> <event> <expected version or -> <label>
change_line() {
  local lines all detail="" count
  lines=$(grep "^$1 $2 " <<< "$events" || true)
  all=$(grep -c . <<< "$lines" || true)
  for version in v1 v2 empty other; do
    count=$(grep -c " $version " <<< "$lines" || true)
    [ "$count" -gt 0 ] && [ "$3" != - ] && detail="$detail, $count $version"
  done
  local verdict=ok
  [ "$all" -eq 50 ] || verdict="<- $((50 - all)) missing"
  if [ "$3" != - ] && [ "$all" -eq 50 ]; then
    count=$(grep -c " $3 " <<< "$lines" || true)
    [ "$count" -eq 50 ] || verdict="<- $((50 - count)) not $3"
  fi
  printf '  %-34s ->  %-26s %s\n' "$4" "$all events$detail" "$verdict"
}

# end_line <phase> <expected last event> <label>: the last event per row decides the target's state
end_line() {
  local count; count=$(grep -c "^$1 $2$" <<< "$last" || true)
  printf '  %-34s ->  %-26s %s\n' "$3" "last event: $count $2" "$([ "$count" -eq 50 ] && echo ok || echo "<- $((50 - count)) rows end wrong")"
}

# count <phase> [ok] from kafka_rows output on stdin: rows in Kafka, or rows with a correct CLOB
count() { grep "^$run:$1:[0-9]* ${2:-}" | cut -d' ' -f1 | sort -u | wc -l; }

in_kafka() { kafka_rows | count "$1"; }

step() { echo "$(date +%T)  $*"; }

state() { curl -s "$api/connectors/$run/status" | jq -r '.tasks[0].state // "STARTING"'; }

# why the task failed: the known Debezium messages, otherwise the first "Caused by" line
reason() {
  local trace; trace=$(curl -s "$api/connectors/$run/status" | jq -r '.tasks[0].trace // ""')
  grep -m1 -o -E 'Oracle LogMiner is unable.*|no longer available.*|Invalid value: .*' <<< "$trace" \
    || grep -m1 'Caused by' <<< "$trace" || true
}

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
# grep -c reads the whole log; grep -q would stop early and fail the pipeline with SIGPIPE
streaming() { [ "$(docker compose logs --since "$since" connect 2>/dev/null | grep -c 'Starting streaming')" -gt 0 ]; }
all_delivered() { [ "$(in_kafka "$1")" -eq "$(in_db "$1")" ]; }

start_connector() {
  step "waiting for Kafka Connect"
  wait_for 300 api_up
  step "starting connector $run"
  local t0; t0=$(date -u +%FT%TZ)
  for old in $(curl -s "$api/connectors" | jq -r '.[]'); do curl -s -X DELETE "$api/connectors/$old"; done
  jq -n --args '$ARGS.positional | map(split("=") | {(.[0]): (.[1:] | join("="))}) | add // {}' "$@" \
    | jq --arg run "$run" --slurpfile base <(jq -s add "${configs[@]}") \
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

# restart a FAILED task up to 3 times, as an external auto-restart would. The offset stored at the
# first failure is what a restart must resume from.
auto_restart() {
  local failed=no
  for i in 1 2 3; do
    step "checking task in 20 s ($i/3)"
    sleep 20
    running && { step "task RUNNING"; continue; }
    step "task FAILED: $(reason)"
    [ $failed = yes ] || { stored=$(stored_scn); failed=yes; }
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
  [ "$(state)" = FAILED ] && echo "      $(reason)"
  printf '%-8s %6s %6s %8s\n' phase db kafka clob-ok
  for phase in "$@"; do
    printf '%-8s %6s %6s %8s\n' "$phase" "$(in_db "$phase")" "$(count "$phase" <<< "$rows")" "$(count "$phase" ok <<< "$rows")"
  done
}

echo "== $scenario ${configs[*]} ${*:-} ($run)"
case "$scenario" in ddl*) echo "DDL: $ddl" ;; esac
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

  changes)
    step "stopping Kafka Connect, so every CLOB is read back late"
    docker compose stop connect >/dev/null 2>&1
    step "50 rows (kept): insert, update with new CLOB, update without CLOB"
    insert kept 50
    change kept
    step "50 rows (deleted): the same, then delete"
    insert deleted 50
    change deleted
    delete_rows deleted
    step "starting Kafka Connect, waiting for streaming"
    docker compose start connect >/dev/null 2>&1
    wait_for 300 streaming
    sleep 30
    resume_point
    change_report
    ;;

  archive-gone)
    step "stopping Kafka Connect"
    docker compose stop connect >/dev/null 2>&1
    step "inserting 50 rows while it is down (gone)"
    insert gone 50
    step "switching logs and deleting all archive logs with RMAN"
    drop_archives
    step "starting Kafka Connect"
    docker compose start connect >/dev/null 2>&1
    wait_for 300 api_up
    sleep 20
    wait_for 120 started
    [ "$(state)" = FAILED ] && step "task FAILED: $(reason)"
    step "inserting 50 rows (after), waiting 20 s"
    insert after 50
    sleep 20
    resume_point
    report before gone after
    ;;

  reorg)
    secs=${REORG_SECONDS:-300}
    for logger in logminer.AbstractLogMinerStreamingChangeEventSource logminer.buffered.BufferedLogMinerStreamingChangeEventSource; do
      curl -s -X PUT -H 'Content-Type: application/json' -d '{"level": "DEBUG"}' \
        "$api/admin/loggers/io.debezium.connector.oracle.$logger" >/dev/null
    done
    redo0=$(redo_mb)
    step "reorg transaction open for $secs s, meanwhile 1 row per second (during)"
    reorg "$secs" &
    insert during "$secs" 1
    wait
    step "reorg committed, $(( $(redo_mb) - redo0 )) MB redo; inserting 5 rows (after)"
    # A lost row never arrives, so waiting for all of during would hang. Once after is in Kafka,
    # the connector has read past every during row; what is missing then stays missing.
    insert after 5
    wait_for 1800 all_delivered after
    report during after
    echo "missing         $(missing during)"
    session_report
    lag_report during
    ;;

  *) echo "unknown scenario: $scenario" >&2; exit 1 ;;
esac
