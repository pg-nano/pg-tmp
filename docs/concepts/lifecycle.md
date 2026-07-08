# Lifecycle

> Decide which process owns initialization, startup, delayed shutdown, and
> deletion of the temporary PostgreSQL container.

`pg-tmp` separates the public container root from the actual PostgreSQL cluster:

```text
/tmp/pg_tmp.xxxxxx/
  NEW
  initdb.log
  stop.log
  data/
    PG_VERSION
    postgresql.conf
    postgres.log
```

The root directory is the value returned as `dataDir`. PostgreSQL owns the
`data/` directory. `PG_VERSION` inside that directory is the compatibility check
used when `start()` looks for a prewarmed cluster.

## Initialization

`initdb()` creates the root container and initializes `data/` for temporary use:

```ts
import { initdb } from '@pg-nano/pg-tmp'

const dataDir = await initdb()
```

The cluster is configured for speed over durability. It uses trust
authentication, disables fsync-related durability settings, writes logs inside
the container, and listens on a Unix socket by default.

Calling `initdb()` on a root that already has `data/PG_VERSION` throws instead
of reusing the existing cluster.

## Startup

`start()` does not require a prior `initdb()` call:

```ts
import { start } from '@pg-nano/pg-tmp'

const pg = await start()
```

When `dataDir` is omitted, `start()` first scans `os.tmpdir()` for compatible
`pg_tmp.*` roots with a `NEW` marker. If it claims one, startup can skip
initialization. If none exists, it initializes a fresh root.

After claiming or creating a root, `start()` begins initializing another root in
the background. That prewarmed root is available to a later `start()` call.

## Shutdown

There are two shutdown paths:

| Path | Owner | Cleanup |
| --- | --- | --- |
| `await pg.stop()` | Current process | Stops Postgres and removes the root unless `keep` is true. |
| `timeout > 0` | Background stopper | Waits for active `test` connections, then stops and removes the root unless `keep` is true. |

Use manual shutdown for tests where the process has a clear `finally` block:

```ts
import { start } from '@pg-nano/pg-tmp'

const pg = await start({ timeout: 0 })

try {
  await runTests(pg.dsn)
} finally {
  await pg.stop()
}
```

Use background shutdown for scripts that hand a DSN to another process and then
exit:

```ts
import { start } from '@pg-nano/pg-tmp'

const pg = await start({
  timeout: 60,
})

console.log(pg.dsn)
```

If active connections remain when the timeout expires, the background stopper
keeps waiting and checks again on the same interval.
