#!/usr/bin/env bash
# why-missing.sh <run> <phase> <row...>: for each lost row, its transaction, how Debezium committed
# it, and whether a mining window ended at a log boundary right after the transaction started.
run=$1 phase=$2; shift 2
log=$(docker compose logs connect 2>/dev/null)
bounds=$(docker compose exec -T oracle sqlplus -s / as sysdba <<< "SET HEADING OFF FEEDBACK OFF PAGES 0
SELECT next_change# FROM v\$archived_log WHERE dest_id = 1;" | tr -d ' ')
for n in "$@"; do
  read -r scn xid < <(docker compose exec -T oracle sqlplus -s app/app@localhost:1521/FREEPDB1 <<< "SET HEADING OFF FEEDBACK OFF PAGES 0
SELECT VERSIONS_STARTSCN||' '||VERSIONS_XID FROM APP.EVENTS VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE WHERE EV_NAME = '$run:$phase:$n' AND VERSIONS_OPERATION = 'I';")
  commit=$(grep -i "Committing transaction $xid " <<< "$log" | grep -o 'with [0-9]* events' | head -1)
  # the windows that ended between the transaction's start (commit - 100) and its commit
  ends=$(grep -o 'Fetching results for SCN \[[0-9]*, [0-9]*\]' <<< "$log" | grep -o '[0-9]*\]' | tr -d ']' \
    | awk -v c="$scn" '$1 < c && $1 > c - 100')
  atlog=$(for e in $ends; do grep -qx "$e" <<< "$bounds" && echo "$e"; done | tr '\n' ' ')
  echo "row $n  xid $xid  commit scn $scn  debezium: ${commit:-no commit logged}  window end at log boundary: ${atlog:-none}"
done
