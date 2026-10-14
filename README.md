# Debezium Oracle: Restart and DDL Data Loss

Two things lose change events in the production setup:

1. **Every restart skips the downtime.** With `snapshot.mode=configuration_based` and
   `snapshot.schema=true`, each task start takes a schema snapshot and continues at the current
   SCN instead of the stored offset.
2. **A DDL on a captured table stops the connector** if it is even a few seconds behind:
   `Oracle LogMiner is unable to re-construct the SQL for 'INSERT' event`. The next restart
   then skips everything since the stored offset.

The recommended configuration loses nothing in either case.

## Run

Needs Docker, `jq` and `curl`.

```bash
docker compose up -d                         # first start ~3 min
./run.sh restart production.json             # restart
./run.sh ddl production.json                 # DDL while the connector is a few seconds behind
./run.sh ddl-caught-up production.json       # DDL while the connector is idle
./run.sh ddl recommended.json                # any scenario, recommended configuration
docker compose down -v                       # remove everything
```

| File | Configuration |
|---|---|
| `production.json` | as in production: `lob.enabled=true`, `online_catalog`, filter mode `in`, CLOB reselector, `snapshot.mode=configuration_based` with `snapshot.schema=true` |
| `recommended.json` | the same with `snapshot.mode=no_data`, `log.mining.strategy=hybrid`, `lob.enabled=false`, `unavailable.value.placeholder=""` |

Single keys can be overridden after the file, e.g. `./run.sh ddl recommended.json lob.enabled=true`.
`DDL="..."` replaces the DDL; `COL` becomes a fresh column name. Each run creates a fresh
connector.

## Scenarios

Every row gets an 8 KB CLOB that starts with the row's name, so the report can count rows whose
CLOB arrived intact (`clob-ok`). Every run first inserts 50 rows (`before`) and waits until they
are in Kafka and the offset is stored. Then:

| Scenario | What happens |
|---|---|
| `restart` | Kafka Connect is stopped, 50 rows are inserted (`down`), Connect starts, 50 more rows (`after`) |
| `ddl` | 100 rows are inserted at 10 per second (`during`); after 5 s the DDL runs; a FAILED task is restarted up to 3 times, like an external auto-restart; 50 more rows (`after`) |
| `ddl-caught-up` | the DDL runs, then 50 rows (`after`) |

## Results (Debezium 3.7.0)

Rows of the `down`, `during` or `after` phase in Kafka, with their own CLOB:

| Run | `production.json` | `recommended.json` |
|---|---|---|
| `restart` | **0 / 50**, resumed at 2301595 instead of 2300998 | 50 / 50 |
| `ddl`, `ADD COL VARCHAR2(40 CHAR)` | **36 / 100**, failed, restart skipped to 2304178 | 100 / 100, never failed |
| `ddl`, `ADD (COL … DEFAULT 'N' NOT NULL, COL_C …, CONSTRAINT … CHECK …)` | **33 / 100**, failed, restart skipped to 2309012 | 100 / 100, never failed |
| `ddl-caught-up` | 50 / 50 | 50 / 50 |

Each run prints `stored offset` (where the connector stopped) and `resumed from` (where it
continued). If they differ, everything in between is skipped.

Debezium 3.6.1 behaves the same, with one exception: it cannot parse `ADD (…, CONSTRAINT …
CHECK …)` and fails with `Invalid value: null used for required field: "typeName"`, in both
configurations and even when caught up ([dbz#2405](https://github.com/debezium/dbz/issues/2405),
fixed in 3.7.0).

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
