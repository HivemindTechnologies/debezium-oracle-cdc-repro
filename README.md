# Debezium Oracle: Restart and DDL Data Loss

Two things lose change events in the production setup:

1. **Every restart skips the downtime.** With `snapshot.mode=configuration_based` and
   `snapshot.schema=true`, each task start takes a schema snapshot and continues at the current
   SCN instead of the stored offset.
2. **A DDL on a captured table stops the connector** if it is even a few seconds behind:
   `Oracle LogMiner is unable to re-construct the SQL for 'INSERT' event`. The next restart
   then skips everything since the stored offset.

`snapshot.mode=no_data` fixes the first and turns the second into a connector that stays
FAILED, with nothing lost.

## Run

Needs Docker, `jq` and `curl`.

```bash
docker compose up -d            # first start ~3 min
./run.sh restart                # restart, production config
./run.sh restart snapshot.mode=no_data
./run.sh ddl                    # DDL while the connector is a few seconds behind
./run.sh ddl snapshot.mode=no_data
./run.sh ddl-caught-up          # DDL while the connector is idle
docker compose down -v          # remove everything
```

`DDL="..."` replaces the DDL of the `ddl` scenarios; `COL` becomes a fresh column name:

```bash
DDL="ALTER TABLE APP.EVENTS ADD (COL VARCHAR2(1 CHAR) DEFAULT 'N' NOT NULL)" ./run.sh ddl
```

`connector.json` holds the tested configuration, close to production (`lob.enabled=true`,
`online_catalog`, filter mode `in`, the CLOB reselector, the production snapshot settings). Edit it, or override
single keys on the command line as above. Each run creates a fresh connector.

## Scenarios

Every row gets an 8 KB CLOB that starts with the row's name, so the report can count rows whose
CLOB arrived intact (`clob-ok`). Every run first inserts 50 rows (`before`) and waits until they
are in Kafka and the offset is stored. Then:

| Scenario | What happens |
|---|---|
| `restart` | Kafka Connect is stopped, 50 rows are inserted (`down`), Connect starts, 50 more rows (`after`) |
| `ddl` | 100 rows are inserted at 10 per second (`during`); after 5 s `ALTER TABLE … ADD … VARCHAR2(40 CHAR)` runs; a FAILED task is restarted up to 3 times, like an external auto-restart; 50 more rows (`after`) |
| `ddl-caught-up` | the same `ALTER TABLE`, then 50 rows (`after`) |

## Results (Debezium 3.6.1)

Rows of the `down` or `during` phase that reached Kafka, and how many of them carried their own
CLOB (out of 50 or 100):

| Run | Task at the end | stored → resumed SCN | down / during | CLOB ok |
|---|---|---|---|---|
| `restart` | RUNNING | 2748084 → **2748685** | **0 / 50** | 0 |
| `restart snapshot.mode=no_data` | RUNNING | 2749305 → 2749305 | 50 / 50 | 50 |
| `ddl` | RUNNING after 1 restart | 2761861 → **2762321** | **26 / 100** | 26 |
| `ddl snapshot.mode=no_data` | FAILED on every restart | 2759311 → 2759311 | 40 / 100, rest waiting | 40 |
| `ddl snapshot.mode=no_data log.mining.strategy=hybrid lob.enabled=false` | RUNNING, never failed | | 100 / 100 | **86** |
| `ddl snapshot.mode=no_data log.mining.strategy=hybrid lob.enabled=false unavailable.value.placeholder=` | RUNNING, never failed | | 100 / 100 | **100** |
| `restart snapshot.mode=no_data log.mining.strategy=hybrid lob.enabled=false` | RUNNING | 2757292 → 2757292 | 50 / 50 | 50 |
| `ddl-caught-up` | RUNNING, never failed | | 50 / 50 (`after`) | 50 |

`stored` is where the connector stopped, `resumed` where it continued. If they differ,
everything in between is skipped.

With `hybrid`, the rows written just before the DDL and read after it (14 and 12 rows in two
runs) arrive with an empty string as CLOB, and the reselector only replaces NULL and the
placeholder. Setting the placeholder to the empty string (`unavailable.value.placeholder=`)
makes the reselector replace those values as well: 100 of 100 in two runs.

## Why

- `ConfigurationBasedSnapshotter.shouldSnapshotSchema()` returns the configured `true` on every
  start, also when an offset exists. `no_data` returns `!offsetExists`.
- `OracleSnapshotChangeEventSource.determineSnapshotOffset()` keeps the stored offset only if
  `shouldStreamEventsStartingFromSnapshot()` is `false`. `ConfigurationBasedSnapshotter`
  inherits the default `true`, so the snapshot's current SCN replaces the offset.
- With `online_catalog`, LogMiner decodes old redo with the table's current definition. Rows
  written before the DDL and read after it come back as `Dictionary Mismatch`, permanently.

## Limits

The `ddl` scenario uses a plain `ADD COLUMN`; other DDL is untested here.
