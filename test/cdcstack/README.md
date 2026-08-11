# Local CDC stack

The full TiDB + TiCDC + Pulsar pipeline, for running cdcfresh locally against
real change data — write a row in TiDB, watch a rebuild fire. It is also how
the canal-json fixtures in `internal/canaljson/testdata/` get captured.

Single-node everything, no volumes: state dies with the containers.

The integration tier does **not** use this stack. Those tests start their own
single containers through `internal/testenv`, so `go test -tags=integration
./...` works with nothing running here. Bring this up when you want the real
pipeline in front of you, not to run the suite.

## Bring it up

```
make infra-up
```

That starts the cluster and runs `bootstrap.sh`, which waits for TiDB, TiCDC
and Pulsar to report healthy and then creates a changefeed sinking
**canal-json** to Pulsar. Re-running it is safe: an existing changefeed is left
alone. Cold start pulls roughly 2 GB of images and takes a few minutes.

| Endpoint | Address |
|---|---|
| TiDB (MySQL protocol) | `127.0.0.1:4000`, user `root`, no password |
| TiDB status/health | `http://127.0.0.1:10080/status` |
| TiCDC API | `http://127.0.0.1:8300` |
| Pulsar broker | `pulsar://127.0.0.1:6650` |
| Pulsar admin | `http://127.0.0.1:8080` |
| Topic | `persistent://public/default/cdcfresh` |

Then produce changes and read what lands on the topic:

```bash
# 1. produce changes. The TiDB image ships no mysql client, so use a throwaway
#    one on the compose network (or your own client against 127.0.0.1:4000).
docker run --rm --network cdcfresh-stack_default mysql:8 \
  mysql -h tidb -P 4000 -u root -e "
    CREATE DATABASE IF NOT EXISTS demo;
    CREATE TABLE IF NOT EXISTS demo.readings (id INT PRIMARY KEY, device VARCHAR(64), value INT);
    INSERT INTO demo.readings VALUES (1, 'dev-a', 10);
    UPDATE demo.readings SET value = 11 WHERE id = 1;
    DELETE FROM demo.readings WHERE id = 1;"

# 2. drain the topic (each -s subscription name reads independently)
docker compose -f test/cdcstack/docker-compose.yml exec -T pulsar \
  bin/pulsar-client consume persistent://public/default/cdcfresh \
  -s golden-capture -n 5 -p Earliest
```

Save one message per event type into `internal/canaljson/testdata/`, named for
what it is (`insert.json`, `update.json`, `delete.json`, `ddl_create.json`,
`ddl_query.json`). Strip nothing: the decoder must tolerate the payload exactly
as TiCDC emits it. `make infra-down` tears the stack down.

What the captured samples show, and what the decoder therefore has to handle:

- `data` and `old` are **arrays** of row objects, not single objects — one
  message can carry several rows, so a decoder must fan out rather than assume
  one row per message.
- Every column value is a **string**, including integers (`"value":"10"`).
- There is no `commitTs` field. Timestamps are `es` (event time, ms) and `ts`
  (the time TiCDC processed it).
- `DELETE` carries the removed row in `data`, and `old` is null.
- DDL arrives with `isDdl: true` and a `type` of `QUERY` or `CREATE`; these are
  dropped by default rather than turned into dirty keys.
- The changefeed is cluster-wide, so the topic carries DDL and unrelated
  tables alongside what you want. Match on decoded `isDdl` and `type`, never on
  a table name appearing in the payload text — `CREATE TABLE` names the table
  too, which is enough to fool a substring check.

## Troubleshooting

**TiKV exits one second after start.** It refuses to boot below ~123k open
files (`the maximum number of open file descriptors is too small`), which the
compose file raises explicitly. Docker Desktop is generous by default and Linux
CI runners cap at 65536, so removing that `ulimits` block breaks CI while
looking fine locally. Everything downstream then waits on a dead cluster, so
check `docker compose ps` for an exited container before reading timeouts as
slowness.

**TiKV or TiDB restarting.** The cluster needs a few GB of memory; check
Docker's memory allocation before suspecting the config.

**Changefeed exists but nothing arrives on the topic.** Check its state with
`docker compose -f test/cdcstack/docker-compose.yml exec -T ticdc /cdc cli
changefeed query --server=http://127.0.0.1:8300 --changefeed-id=cdcfresh`. A
changefeed that hit an error stays stopped until it is resumed or recreated.

**Port already in use.** The stack binds 4000, 8300, 6650, 8080 and 10080 on
the host. Nothing here is precious — stop whatever else holds the port, or edit
the compose file locally.
