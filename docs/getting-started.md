# Getting Started

> Start a temporary PostgreSQL server, connect to the `test` database, and stop
> it with the cleanup behavior you intend.

Install the package:

```bash
pnpm add @pg-nano/pg-tmp
```

`pg-tmp` uses locally installed PostgreSQL binaries through `local-postgres`.
The `postgres` and `initdb` binaries must be available on `PATH`.

## Start And Stop Manually

Use `timeout: 0` when the current process owns cleanup:

```ts
import { start } from '@pg-nano/pg-tmp'

const pg = await start({ timeout: 0 })

try {
  await runMigrations(pg.dsn)
  await runTests(pg.dsn)
} finally {
  await pg.stop()
}
```

`pg.dsn` points at the `test` database. In the default socket mode, it has this
shape:

```text
postgresql:///test?host=/tmp/pg_tmp.xxxxxx/data
```

## Listen On TCP

Use TCP when a client cannot use Unix sockets:

```ts
const pg = await start({
  host: true,
  timeout: 0,
})

console.log(pg.dsn)
// postgresql://127.0.0.1:54321/test
```

Pass a host string for a custom bind address:

```ts
await start({
  host: '127.0.0.1',
  port: 55432,
  timeout: 0,
})
```

## Let pg-tmp Clean Up Later

When `timeout` is positive, `start()` spawns a background stopper. The stopper
waits for active `test` database connections to finish before stopping Postgres
and deleting the temp container.

```ts
const pg = await start({
  timeout: 30,
})

console.log(pg.dsn)
```

Use `keep: true` when you want to inspect the container after Postgres stops:

```ts
const pg = await start({
  timeout: 10,
  keep: true,
})

console.log(pg.dataDir)
```

The actual PostgreSQL cluster lives at `path.join(pg.dataDir, 'data')`.
