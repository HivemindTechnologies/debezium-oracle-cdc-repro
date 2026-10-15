# Debezium Oracle: Restart and DDL Data Loss

Two things lose change events in the production setup:

1. **Every restart skips the downtime.** With `snapshot.mode=configuration_based` and
   `snapshot.schema=true`, each task start takes a schema snapshot and continues at the current
   SCN instead of the stored offset.
2. **A DDL on a captured table stops the connector** if it is even a few seconds behind:
   `Oracle LogMiner is unable to re-construct the SQL for 'INSERT' event`. The next restart
   then skips everything since the stored offset.

Four layers on top of `production.json` fix this; with all four, nothing is lost.

## Run

Needs Docker, `jq` and `curl`.

```bash
docker compose up -d        # first start ~3 min
docker compose down -v      # remove everything at the end
```

`./run.sh <scenario> production.json [layer.json ...] [key=value ...]` creates a fresh connector,
runs the scenario and prints what reached Kafka. `production.json` is the configuration as in
production: `lob.enabled=true`, `online_catalog`, filter mode `in`, the CLOB reselector,
`snapshot.mode=configuration_based` with `snapshot.schema=true`. Each layer file holds one fix and
is merged on top, left to right; `key=value` pairs override single keys last.

| Layer | Setting | Fixes |
|---|---|---|
| `1-no-data.json` | `snapshot.mode=no_data` | a restart resumes at the stored offset instead of skipping ahead |
| `2-lob-disabled-reselector.json` | `lob.enabled=false` | CLOBs come from the reselector; needed for layer 3 |
| `3-hybrid.json` | `log.mining.strategy=hybrid` | a DDL no longer stops the connector |
| `4-empty-placeholder.json` | `unavailable.value.placeholder=""` | rows rebuilt by `hybrid` get their CLOB from the reselector |

## Journey

Each command runs on its own; copy them in order. The comment says what to look for.

### 1. A Restart Loses Data

```bash
./run.sh restart production.json
```
`down 50 0`: the 50 rows written while Connect was down never arrive; `resumed from` is higher
than `stored offset`.

```bash
./run.sh restart production.json 1-no-data.json
```
`down 50 50`: the restart resumes at the stored offset.

### 2. A DDL Stops the Connector

```bash
./run.sh ddl production.json
```
`task FAILED: … unable to re-construct the SQL …`, then the auto-restart skips ahead: most
`during` rows are lost.

```bash
./run.sh ddl production.json 1-no-data.json
```
The task stays FAILED on every restart. Nothing is lost, but nothing moves either.

```bash
./run.sh ddl production.json 1-no-data.json 3-hybrid.json
```
Rejected at start: `hybrid` is not compatible with `lob.enabled=true`.

```bash
./run.sh ddl production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json
```
The DDL passes and all 100 `during` rows arrive, but about 14 of them with an empty CLOB.

```bash
./run.sh ddl production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
All rows, all CLOBs.

### 3. Updates and Deletes With the Reselector

```bash
./run.sh changes production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
Every line ends in `ok`: each change in the table arrives with its CLOB version, and every row
ends correct (kept rows on v2, deleted rows with a delete).

```bash
./run.sh changes production.json
```
`0 events` on every line: the restart skipped all changes.

### 4. Archive Logs Deleted While the Connector Is Down

```bash
./run.sh archive-gone production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
`task FAILED: no longer available on the server`. It waits for someone to act.

```bash
./run.sh archive-gone production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json snapshot.mode=when_needed
```
A full snapshot of the table, then streaming: all 50 `gone` rows arrive.

### 5. A Constraint in the Same Statement (Debezium 3.6.1)

```bash
DDL="ALTER TABLE APP.EVENTS ADD (COL VARCHAR2(1 CHAR) DEFAULT 'N' NOT NULL, CONSTRAINT COL_CK CHECK (COL IN ('N','Y')))" \
  ./run.sh ddl-caught-up production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
FAILED with `Invalid value: null used for required field: "typeName"`, even with no lag.

```bash
DDL=$'ALTER TABLE APP.EVENTS ADD (COL VARCHAR2(1 CHAR) DEFAULT \'N\' NOT NULL);\nALTER TABLE APP.EVENTS ADD CONSTRAINT COL_CK CHECK (COL IN (\'N\',\'Y\'))' \
  ./run.sh ddl production.json 1-no-data.json 2-lob-disabled-reselector.json 3-hybrid.json 4-empty-placeholder.json
```
The same change as two statements passes, also with writes running.

`DDL="..."` replaces the default DDL (`ADD COL VARCHAR2(40 CHAR)`); `COL` becomes a column name
unique to the run.

## Scenarios

Every row gets an 8 KB CLOB that starts with the row's name and version (`…:7:x…` for v1,
`…:7:v2:x…` for v2), so the report can tell which CLOB each event carried. Every run first inserts 50 rows (`before`) and waits until they
are in Kafka and the offset is stored. Then:

| Scenario | What happens |
|---|---|
| `restart` | Kafka Connect is stopped, 50 rows are inserted (`down`), Connect starts, 50 more rows (`after`) |
| `ddl` | 100 rows are inserted at 10 per second (`during`); after 5 s the DDL runs; a FAILED task is restarted up to 3 times, like an external auto-restart; 50 more rows (`after`) |
| `ddl-caught-up` | the DDL runs, then 50 rows (`after`) |
| `changes` | Kafka Connect is stopped; 50 rows (`kept`) are inserted, updated with a new CLOB, updated without touching the CLOB; 50 more rows (`deleted`) get the same and are then deleted; Connect starts. The report shows each change in the table next to what reached Kafka, then the end state per row |
| `archive-gone` | Kafka Connect is stopped, 50 rows are inserted (`gone`), the logs are switched and every archive log is deleted with RMAN, Connect starts, 50 more rows (`after`) |

## Results (Debezium 3.7.0)

Rows of the `down`, `during` or `after` phase in Kafka, with their own CLOB:

| Run | `production.json` | with all four layers |
|---|---|---|
| `restart` | **0 / 50**, resumed at 2301595 instead of 2300998 | 50 / 50 |
| `ddl`, `ADD COL VARCHAR2(40 CHAR)` | **36 / 100**, failed, restart skipped to 2304178 | 100 / 100, never failed |
| `ddl`, `ADD (COL … DEFAULT 'N' NOT NULL, COL_C …, CONSTRAINT … CHECK …)` | **33 / 100**, failed, restart skipped to 2309012 | 100 / 100, never failed |
| `ddl-caught-up` | 50 / 50 | 50 / 50 |

Each run prints `stored offset` (where the connector stopped) and `resumed from` (where it
continued). If they differ, everything in between is skipped.

### Updates and Deletes (`changes`, Debezium 3.6.1, all four layers)

Every event of the 100 rows arrived, 399 of 400 with their own CLOB version. One insert event
carried the CLOB of the later update: the old version was gone (`ORA-01555`), so Debezium read the
current value instead (`Trying to perform re-selection without flashback`). The target ends up
correct, because the update follows. Events of deleted rows still found their CLOB through the
flashback read.

### Archive Logs Gone (`archive-gone`, Debezium 3.6.1)

| `snapshot.mode` | Result | `gone` rows in Kafka |
|---|---|---|
| `configuration_based` (`production.json`) | FAILED: `no longer available on the server` | 0, `after` 0 |
| `no_data` (layer 1) | FAILED: `no longer available on the server` | 0, `after` 0 |
| `when_needed` | full snapshot of the table, then streaming | 50 / 50, with CLOBs |

`when_needed_no_data` exists in Debezium, but the Oracle connector does not accept it.
On a normal restart `when_needed` resumes at the stored offset like `no_data`.

### Debezium Versions

The compose file runs 3.6.1. Each version has one bug that matters here:

| | 3.6.1 | 3.7.0 |
|---|---|---|
| `ADD (col …, CONSTRAINT … CHECK …)` in one statement | fails with `Invalid value: null used for required field: "typeName"`, in every configuration ([dbz#2405](https://github.com/debezium/dbz/issues/2405)); two statements work | works |
| CLOB read-back when the old version is gone (`changes` on a fresh database) | reads the current value instead, keeps running | **task fails on every restart**: `Couldn't read binary data for column EV_DATA … ORA-01555` |

In 3.7.0 the reselector reads the CLOB after the query has returned (the new reselect cache),
outside the fallback that handles `ORA-01555`.

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
