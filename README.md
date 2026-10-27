# Debezium Oracle CDC: Where Change Events Get Lost

A local Oracle, Kafka and Debezium 3.6.3 setup that reproduces five ways an Oracle LogMiner
connector loses or stops delivering change events, and the configuration that fixes each:

1. **A restart skips the downtime** with `snapshot.mode=configuration_based` and
   `snapshot.schema=true`.
2. **A DDL on a captured table stops the connector** if it is even a few seconds behind, and
   the next restart skips ahead.
3. **CLOBs read back after the fact** (`lob.enabled=false`) must still arrive in the right
   version for updates and deletes.
4. **Deleted archive logs** make the stored offset unreachable.
5. **A CLOB insert split by a log switch** is skipped silently with `lob.enabled=false`; a long
   uncaptured transaction, such as a reorg job, makes this frequent.

## Run

Needs Docker, `jq` and `curl`.

```bash
docker compose up -d        # first start ~3 min
docker compose down -v      # remove everything at the end
```

`./run.sh <scenario> 0-production.json [layer.json ...] [key=value ...]` creates a fresh connector,
runs the scenario and prints what reached Kafka. `0-production.json` is the starting
configuration: `lob.enabled=true`, `online_catalog`, filter mode `in`, the CLOB reselector,
`snapshot.mode=configuration_based` with `snapshot.schema=true`. Each layer file holds one fix and
is merged on top, left to right; `key=value` pairs override single keys last.

| Layer | Setting | Fixes |
|---|---|---|
| `1-no-data.json` | `snapshot.mode=no_data` | a restart resumes at the stored offset instead of skipping ahead |
| `2-lob-disabled-reselector.json` | `lob.enabled=false` | CLOBs come from the reselector; needed for layer 3 |
| `3-hybrid.json` | `log.mining.strategy=hybrid` | a DDL no longer stops the connector |
| `4-empty-placeholder.json` | `unavailable.value.placeholder=""` | rows rebuilt by `hybrid` get their CLOB from the reselector |
| `5-deferred-start.json` | `log.mining.buffer.deferred.transaction.start=true` | a long uncaptured transaction no longer pins the mining session; no CLOB insert lost in step 6 |

Every run first inserts 50 rows (`before`) and waits until they are in Kafka and the offset is
stored. Each row carries an 8 KB CLOB that starts with the row's name and version (`…:7:x…` for
v1, `…:7:v2:x…` for v2), so the report can tell which CLOB each event carried. `clob-ok` counts
rows whose CLOB arrived intact. `stored offset` is where the connector stopped, `resumed from`
where it continued; if they differ, everything in between is skipped.

## Journey

Each command runs on its own; copy them in order. The output of steps 1 to 5 is from one run on
Debezium 3.6.1; 3.6.3 gives the same results for `restart`, `ddl` and `changes`, and passes
step 5. Step 6 is from 3.6.3. Counts marked *varies* depend on how far the connector lags when the DDL runs.

### 1. A Restart Loses Data

Kafka Connect is stopped, 50 rows are inserted (`down`), Connect starts, 50 more rows (`after`).

```bash
./run.sh restart 0-production.json
```
```
stored offset   scn=2261600
resumed from    scn=2263399       <- skipped ahead
down         50      0        0   <- all 50 lost
```

```bash
./run.sh restart 0-production.json 1-no-data.json
```
```
stored offset   scn=2264219
resumed from    scn=2264219
down         50     50       50
```

### 2. A DDL Stops the Connector

100 rows are inserted at 10 per second (`during`). After 5 s, `ALTER TABLE … ADD … VARCHAR2(40
CHAR)` runs. A FAILED task is restarted up to 3 times, like an external auto-restart; then 50
more rows (`after`).

```bash
./run.sh ddl 0-production.json
```
```
task FAILED: Oracle LogMiner is unable to re-construct the SQL for 'INSERT' event with SCN 2293399
stored offset   scn=2293289
resumed from    scn=2293752       <- the restart skipped ahead
during      100     33       33   <- 67 lost (varies)
```

```bash
./run.sh ddl 0-production.json 1-no-data.json
```
```
task: FAILED                      <- on every restart; nothing lost, nothing moves
stored offset   scn=2295068
resumed from    scn=2295068
during      100     43       43   <- the rest waits (varies)
after        50      0        0
```

```bash
./run.sh ddl 0-production.json 1-no-data.json 3-hybrid.json
```
```
task FAILED: The 'log.mining.strategy' value 'hybrid' is invalid: The hybrid mining strategy is
not compatible when enabling 'lob.enabled'. …
```

```bash
./run.sh ddl 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json
```
```
task: RUNNING                     <- the DDL passes
during      100    100       68   <- 32 rows with an empty CLOB (varies)
```

```bash
./run.sh ddl 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
task: RUNNING
during      100    100      100
```

### 3. Updates and Deletes With the Reselector

Kafka Connect is stopped, so every CLOB is read back late. 50 rows (`kept`) are inserted,
updated with a new CLOB, then updated without touching the CLOB. 50 more rows (`deleted`) get the
same and are then deleted. Connect starts. Each line is one change in the table next to what
reached Kafka for it; `end` is the last event per row, which decides what a target ends up with.

```bash
./run.sh changes 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
kept (50 rows)
  in the table                           in Kafka
  insert, CLOB v1                    ->  50 events, 50 v1           ok
  update, CLOB v2                    ->  50 events, 50 v2           ok
  update, other column               ->  50 events, 50 v2           ok
  end: 50 rows, CLOB v2              ->  last event: 50 v2          ok

deleted (50 rows)
  in the table                           in Kafka
  insert, CLOB v1                    ->  50 events, 50 v1           ok
  update, CLOB v2                    ->  50 events, 50 v2           ok
  update, other column               ->  50 events, 50 v2           ok
  delete                             ->  50 events                  ok
  end: 0 rows                        ->  last event: 50 delete      ok
```

The read-back runs `AS OF SCN <commit>`, so even deleted rows return their old CLOB. When the old
version is already gone (`ORA-01555`), Debezium reads the current value instead: an earlier run
showed `50 events, 49 v1, 1 v2` on the insert line, with `end` still `ok`.

```bash
./run.sh changes 0-production.json
```
```
  insert, CLOB v1                    ->  0 events                   <- 50 missing
  …                                                                 (every line)
  end: 50 rows, CLOB v2              ->  last event: 0 v2           <- 50 rows end wrong
```

### 4. Archive Logs Deleted While the Connector Is Down

Kafka Connect is stopped, 50 rows are inserted (`gone`), the logs are switched and every archive
log is deleted with RMAN, Connect starts, 50 more rows (`after`).

```bash
./run.sh archive-gone 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
task: FAILED
      no longer available on the server. Reconfigure the connector to use a snapshot mode when needed.
gone         50      0        0
after        50      0        0
```

The connector stops loudly; nothing is skipped silently. `0-production.json` stops the same way.
The gap has to be repaired by hand: move the offset, then run an incremental snapshot (not part of
this repro).

### 5. A Constraint in the Same Statement

```bash
DDL="ALTER TABLE APP.EVENTS ADD (COL VARCHAR2(1 CHAR) DEFAULT 'N' NOT NULL, CONSTRAINT COL_CK CHECK (COL IN ('N','Y')))" \
  ./run.sh ddl-caught-up 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
task: FAILED                      <- even with no lag
      Invalid value: null used for required field: "typeName", schema type: STRING
after        50      0        0
```

Debezium 3.6.1 cannot parse the inline constraint
([dbz#2405](https://github.com/debezium/dbz/issues/2405)). The same change as two statements
passes, also with writes running:

```bash
DDL=$'ALTER TABLE APP.EVENTS ADD (COL VARCHAR2(1 CHAR) DEFAULT \'N\' NOT NULL);\nALTER TABLE APP.EVENTS ADD CONSTRAINT COL_CK CHECK (COL IN (\'N\',\'Y\'))' \
  ./run.sh ddl 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
task: RUNNING
during      100    100      100
```

`DDL="..."` replaces the default DDL; `COL` becomes a column name unique to the run.

On 3.6.3 the inline form passes as well: `task: RUNNING`, `during 100 100 100`.

### 6. A Reorg Transaction While CLOB Rows Are Inserted

A user of the uncaptured schema `REORG` keeps one transaction open for 300 s, inserting and
deleting 2 KB rows in a loop, like a reorg job's mass delete: about 1.2 GB of redo, a log switch
every 4 to 5 s with the 20 MB redo logs here. Meanwhile 1 row per second is inserted (`during`),
then 5 rows after the commit (`after`). `missing` lists the rows that never reached Kafka; the
lag lines show how late the events arrived.

```bash
./run.sh reorg 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
```
task: RUNNING                     <- no error, no warning about the lost rows
during      300    298      298
missing         34 214            <- lost (varies)
mining sessions 171, distinct start SCNs 17, widest session 17297 SCNs
  rows 271-300   max lag   12 s
```

```bash
./why-missing.sh r102004 during 34 214    # the run id is printed in the first line
```
```
row 34   xid 0500170090020000  commit scn 2340768  debezium: with 0 events  window end at log boundary: 2340679 2340762
row 214  xid 080004008F020000  commit scn 2344014  debezium: with 0 events  window end at log boundary: 2343955 2344011
```

Debezium committed both transactions with 0 events, right after a mining window that ended
exactly at an archived log's last SCN. Over 600 rows the loss was 6 rows, 2 on 3.6.3 and 4 on
3.6.1 (max lag 44 s on 3.6.1, 13 s on 3.6.3).

```bash
./run.sh reorg 0-production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json 5-deferred-start.json
```
```
task: RUNNING
during      300    300      300
missing
mining sessions 328, distinct start SCNs 116, widest session 624 SCNs
  rows 271-300   max lag    4 s
```

With deferred start: 0 of 900 rows lost in three runs, max lag 3 to 5 s. Also 0 of 300 when
Kafka Connect was stopped for 240 s during the reorg and caught up afterwards. `0-production.json`
alone (`lob.enabled=true`): 0 of 600 lost, max lag 12 s.

## Debezium Version

3.6.3. Compared with 3.6.1 it parses the inline constraint of step 5
([dbz#2405](https://github.com/debezium/dbz/issues/2405), fixed in 3.6.2), stops re-mining the
same logs under heavy redo ([dbz#2326](https://github.com/debezium/dbz/issues/2326)), and fixes
a stale deferred transaction that pinned the offset for up to 24 h
([dbz#2531](https://github.com/debezium/dbz/issues/2531)); layer 5 is safe only with that fix.
The CLOB read-back of step 3 is unchanged from 3.6.1 (`ReselectColumnsPostProcessor` is
identical) and still falls back to a current read on `ORA-01555`.

Not 3.7.0: it breaks the read-back of step 3. When the old CLOB version is gone, the task fails
on every restart with `Couldn't read binary data for column EV_DATA … ORA-01555`. 3.7.0 reads the
CLOB after the query has returned (the new reselect cache), outside the fallback that handles
`ORA-01555`.

## Why

- `ConfigurationBasedSnapshotter.shouldSnapshotSchema()` returns the configured `true` on every
  start, also when an offset exists. `no_data` returns `!offsetExists`.
- `OracleSnapshotChangeEventSource.determineSnapshotOffset()` keeps the stored offset only if
  `shouldStreamEventsStartingFromSnapshot()` is `false`. `ConfigurationBasedSnapshotter`
  inherits the default `true`, so the snapshot's current SCN replaces the offset.
- With `online_catalog`, LogMiner decodes old redo with the table's current definition. Rows
  written before the DDL and read after it come back as `Dictionary Mismatch`, permanently.
  `hybrid` rebuilds them from Debezium's own schema history instead.
- `hybrid` rebuilds the CLOB of such rows as an empty string. The reselector replaces only NULL
  and the placeholder, so the placeholder is set to the empty string as well.
- LogMiner returns a CLOB insert only once all its LOB redo is in the mined logs. If the LOB redo
  continues in the next log, a window ending at the log switch returns only the transaction
  START, at the same SCN as the INSERT. `BufferedLogMinerStreamingChangeEventSource
  .calculateNewStartScn()` moves the read start to that SCN, and the next query reads
  `SCN > ?`, so the INSERT is never read. With `lob.enabled=true` every query reads again from
  the session start, so the INSERT arrives later.
- An open transaction holds the mining session start at its first SCN, also when its schema is
  excluded: START, COMMIT and ROLLBACK records pass the schema filter. The windows then grow
  across archived logs and often end at a log switch. With deferred start a transaction is
  tracked only from its first captured change, so the session follows the current SCN and the
  windows end inside the online log: 0 of 126 windows ended at a log switch, against 35 of 75
  with `lob.enabled=true`. The skip in `calculateNewStartScn()` remains in 3.6.3; deferred start
  only removes its trigger. Likely the same bug: [dbz#2184](https://github.com/debezium/dbz/issues/2184).
