# Debezium Oracle CDC: Where Change Events Get Lost

A local Oracle, Kafka and Debezium 3.6.1 setup that reproduces four ways an Oracle LogMiner
connector loses or stops delivering change events, and the configuration that fixes each:

1. **A restart skips the downtime** with `snapshot.mode=configuration_based` and
   `snapshot.schema=true`.
2. **A DDL on a captured table stops the connector** if it is even a few seconds behind, and
   the next restart skips ahead.
3. **CLOBs read back after the fact** (`lob.enabled=false`) must still arrive in the right
   version for updates and deletes.
4. **Deleted archive logs** make the stored offset unreachable.

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

Every run first inserts 50 rows (`before`) and waits until they are in Kafka and the offset is
stored. Each row carries an 8 KB CLOB that starts with the row's name and version (`…:7:x…` for
v1, `…:7:v2:x…` for v2), so the report can tell which CLOB each event carried. `clob-ok` counts
rows whose CLOB arrived intact. `stored offset` is where the connector stopped, `resumed from`
where it continued; if they differ, everything in between is skipped.

## Journey

Each command runs on its own; copy them in order. The output shown is from one run on
Debezium 3.6.1. Counts marked *varies* depend on how far the connector lags when the DDL runs.

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

## Debezium Version

3.7.0 fixes the constraint parsing, but breaks the read-back of step 3: when the old CLOB version
is gone, the task fails on every restart with `Couldn't read binary data for column EV_DATA …
ORA-01555`. 3.7.0 reads the CLOB after the query has returned (the new reselect cache), outside
the fallback that handles `ORA-01555`. Hence 3.6.1.

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
