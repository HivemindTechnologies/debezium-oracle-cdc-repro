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

`connector.json` holds the tested configuration, close to production (`lob.enabled=true`,
`online_catalog`, filter mode `in`, the production snapshot settings). Edit it, or override
single keys on the command line as above. Each run creates a fresh connector.

## Scenarios

Every run first inserts 50 rows (`before`) and waits until they are in Kafka and the offset is
stored. Then:

| Scenario | What happens |
|---|---|
| `restart` | Kafka Connect is stopped, 50 rows are inserted (`down`), Connect starts, 50 more rows (`after`) |
| `ddl` | 100 rows are inserted at 10 per second (`during`); after 5 s `ALTER TABLE … ADD … VARCHAR2(40 CHAR)` runs; a FAILED task is restarted up to 3 times, like an external auto-restart; 50 more rows (`after`) |
| `ddl-caught-up` | the same `ALTER TABLE`, then 50 rows (`after`) |

## Results (Debezium 3.6.1)

```
== restart, production config               == restart, snapshot.mode=no_data
stored offset   scn=2277590                  stored offset   scn=2279626
resumed from    scn=2278191                  resumed from    scn=2279626
task: RUNNING                                task: RUNNING
phase        db  kafka                       phase        db  kafka
before       50     50                       before       50     50
down         50      0   <- lost             down         50     50
after        50     50                       after        50     50

== ddl, production config                   == ddl, snapshot.mode=no_data
task FAILED: Oracle LogMiner is unable to    task FAILED: Oracle LogMiner is unable to
  re-construct the SQL for 'INSERT' …          re-construct the SQL for 'INSERT' …  (every restart)
stored offset   scn=2284976                  stored offset   scn=2289411
resumed from    scn=2286687                  resumed from    scn=2289411
task: RUNNING                                task: FAILED
phase        db  kafka                       phase        db  kafka
before       50     50                       before       50     50
during      100     17   <- 83 lost          during      100     15   <- waiting, not lost
after        50     50                       after        50      0   <- waiting, not lost

== ddl-caught-up, production config
task: RUNNING
phase        db  kafka
before       50     50
after        50     50
```

`stored offset` is where the connector stopped, `resumed from` is where it continued. If they
differ, everything in between is skipped.

## Why

- `ConfigurationBasedSnapshotter.shouldSnapshotSchema()` returns the configured `true` on every
  start, also when an offset exists. `no_data` returns `!offsetExists`.
- `OracleSnapshotChangeEventSource.determineSnapshotOffset()` keeps the stored offset only if
  `shouldStreamEventsStartingFromSnapshot()` is `false`. `ConfigurationBasedSnapshotter`
  inherits the default `true`, so the snapshot's current SCN replaces the offset.
- With `online_catalog`, LogMiner decodes old redo with the table's current definition. Rows
  written before the DDL and read after it come back as `Dictionary Mismatch`, permanently.

The same code is in Debezium 3.4.1 and 3.6.1.

## Limits

Oracle 23ai Free, one instance, one captured table. Production runs Oracle 19c on RAC with ~120
tables. The `ddl` scenario uses a plain `ADD COLUMN`; other DDL is untested here.
