# pg-tmp

> Use a real local PostgreSQL server for tests, examples, and short-lived
> tooling without owning the low-level server lifecycle yourself.

`@pg-nano/pg-tmp` starts an isolated PostgreSQL server backed by a temporary
container directory. It initializes the cluster for temporary use, creates a
`test` database, returns a connection string, and can remove the container when
the server stops.

The default connection uses a Unix socket inside the temp container:

```ts
import { start } from '@pg-nano/pg-tmp'

const pg = await start({ timeout: 0 })

try {
  console.log(pg.dsn)
} finally {
  await pg.stop()
}
```

Use TCP only when the client library or child process cannot connect through a
Unix socket:

```ts
const pg = await start({
  host: true,
  timeout: 0,
})

await pg.stop()
```

## Start Here

- [Getting started](getting-started.md) shows the smallest install, start, and
  cleanup workflow.
- [Lifecycle](concepts/lifecycle.md) explains container roots, `data/`,
  prewarming, background stops, and cleanup ownership.

## API Source Of Truth

Public TypeScript API behavior is documented with TSDoc in the package entry
point. Use generated declarations for exact signatures, and use these docs for
workflow and lifecycle decisions.
