import { spawn as spawnChild, type StdioOptions } from 'node:child_process'
import { promises as fs, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  ensurePostgresDatabase,
  getPostgresVersion,
  initPostgresDataDir,
  startPostgresDataDir,
  stopPostgresDataDir,
  LocalPostgresError,
  type LocalPostgresLogger,
  type PostgresListenOptions,
} from 'local-postgres/core'
import { noop, sift } from 'radashi'
import { glob } from 'tinyglobby'

const OS_TMP = os.tmpdir()
const isTest = !!process.env.PGTMP_TEST

/**
 * Prefix used for temporary root directories created under `os.tmpdir()`.
 *
 * @remarks
 * `start()` scans directories with this prefix when it looks for a prewarmed
 * cluster that can be claimed with its `NEW` marker.
 */
export const PREFIX = 'pg_tmp.'
const DATA_DIR = 'data'

/**
 * Options for `initdb()`.
 */
export type InitOptions = {
  /**
   * Controls `initdb` output.
   *
   * @remarks
   * Only `'inherit'` is honored. It forwards `initdb` output to the parent
   * process, which is useful when debugging local Postgres installation or
   * initialization failures.
   */
  stdio?: StdioOptions
}

/**
 * Initializes a temporary PostgreSQL root directory.
 *
 * The returned directory is the pg-tmp container root. The actual PostgreSQL
 * cluster is created in its `data` child directory, and the root receives a
 * `NEW` marker so a later `start()` call can claim prewarmed clusters.
 *
 * @param dataDir - Optional container root. When omitted or `null`, a new
 * directory is created under `os.tmpdir()` with the `PREFIX` prefix.
 * @param options - Initialization options.
 * @returns The initialized container root.
 * @throws If the container already has an initialized `data` directory, if the
 * current user cannot initialize it, or if local Postgres binaries are missing
 * or fail.
 *
 * @example
 * ```ts
 * import { initdb, start } from '@pg-nano/pg-tmp'
 *
 * const dataDir = await initdb()
 * const pg = await start({ dataDir, timeout: 0 })
 *
 * await pg.stop()
 * ```
 */
export async function initdb(
  dataDir?: string | null,
  { stdio }: InitOptions = {},
) {
  dataDir ||= await fs.mkdtemp(path.join(OS_TMP, PREFIX))

  const dataPath = path.join(dataDir, DATA_DIR)

  if (await readDataDirectoryVersion(dataDir)) {
    throw new Error(
      `PostgreSQL data directory is already initialized: ${dataPath}`,
    )
  }

  await initPostgresDataDir({
    dataDir: dataPath,
    auth: 'trust',
    encoding: 'UNICODE',
    noSync: true,
    config: {
      unix_socket_directories: dataPath,
      listen_addresses: '',
      shared_buffers: '12MB',
      fsync: false,
      synchronous_commit: false,
      full_page_writes: false,
      log_min_duration_statement: 0,
      log_connections: true,
      log_disconnections: true,
    },
    log: stdio === 'inherit' ? 'inherit' : undefined,
  })

  await fs.writeFile(path.join(dataDir, 'NEW'), '')
  return dataDir
}

/**
 * Options for `start()`.
 */
export type StartOptions = {
  /**
   * pg-tmp container root.
   *
   * @remarks
   * If omitted, `start()` first tries to claim a prewarmed `pg_tmp.*` directory
   * with a compatible `data/PG_VERSION` file and a `NEW` marker. If no
   * compatible prewarmed root exists, it initializes a new one.
   */
  dataDir?: string
  /**
   * Controls whether Postgres listens on TCP instead of the default Unix
   * socket.
   *
   * @remarks
   * Use `true` to listen on `127.0.0.1`, or pass a custom host address. When
   * omitted or `false`, pg-tmp uses a Unix socket in the `data` directory.
   *
   * @default false
   */
  host?: string | boolean
  /**
   * TCP port to listen on when `host` is enabled.
   *
   * @remarks
   * When `host` is enabled and `port` is omitted, an unused local port is
   * selected. Socket mode ignores this option.
   */
  port?: number
  /**
   * Delay (in seconds) before the PostgreSQL instance is
   * automatically stopped. If zero or negative, you are responsible
   * for stopping the database.
   *
   * Note that the instance won't be stopped if active connections
   * exist. In that case, the timeout is restarted and the database
   * continues to run.
   *
   * @default 60
   */
  timeout?: number
  /**
   * Preserve the pg-tmp container root after the background or manual stop.
   *
   * @default false
   */
  keep?: boolean
  /**
   * Extra options passed directly to the `postgres` process.
   *
   * @remarks
   * This string is split into process arguments before startup. Do not set
   * `listen_addresses` or `port`; pg-tmp configures those from `host` and
   * `port`.
   */
  postgresOptions?: string
}

/**
 * Running temporary PostgreSQL server returned by `start()`.
 */
export type PgTmp = {
  /**
   * Connection string for the `test` database.
   *
   * @remarks
   * Socket mode returns `postgresql:///test?host=...`. TCP mode returns
   * `postgresql://host:port/test`.
   */
  dsn: string
  /**
   * pg-tmp container root. The actual PostgreSQL cluster is in `dataDir/data`.
   */
  dataDir: string
  /**
   * Stops Postgres and removes the container root unless `keep` is true.
   */
  stop(options?: StopOptions): Promise<void>
}

/**
 * Starts a temporary PostgreSQL server and returns connection details.
 *
 * `start()` claims or initializes a pg-tmp container root, starts Postgres,
 * ensures the `test` database exists, and schedules a background stop process
 * unless `timeout` is zero or negative.
 *
 * @param options - Startup and lifecycle options.
 * @returns A running server handle with a connection string, container root,
 * and manual `stop()` method.
 * @throws If a provided container has an incompatible `data/PG_VERSION`, if
 * Postgres cannot start, or if the `test` database cannot be created.
 *
 * @example
 * ```ts
 * import { start } from '@pg-nano/pg-tmp'
 *
 * const pg = await start({ timeout: 0 })
 *
 * try {
 *   console.log(pg.dsn)
 * } finally {
 *   await pg.stop()
 * }
 * ```
 */
export async function start(options: StartOptions = {}): Promise<PgTmp> {
  const pgVersion = await getPostgresVersion()
  const pgDataVersion = getPostgresDataVersion(pgVersion)

  let { dataDir } = options

  if (!dataDir) {
    // Look for an existing pg_tmp.* directory that was optimistically
    // initialized by a previous `start` call.
    for (let dir of await glob(PREFIX + '*', { cwd: OS_TMP })) {
      dir = path.join(OS_TMP, dir)

      // Postgres versions must match.
      if ((await readDataDirectoryVersion(dir)) === pgDataVersion) {
        // The 'NEW' file must exist and be owned by the current user.
        const unusedMarker = path.join(dir, 'NEW')
        if (await isOwnedByCurrentUser(unusedMarker)) {
          await fs.rm(unusedMarker).catch(noop)

          dataDir = dir
          break
        }
      }
    }

    // Create a new data directory if none was found.
    if (!dataDir) {
      dataDir = await initdb()
      await fs.rm(path.join(dataDir, 'NEW')).catch(noop)
    }

    // Optimistically initialize another database to speed up future calls.
    backgroundSpawn(
      'node',
      sift([
        isTest && '--experimental-strip-types',
        new URL(`./initdb.${isTest ? 'ts' : 'js'}`, import.meta.url).pathname,
      ]),
    )
  }
  // If a data directory was provided: Initialize the database if a
  // cluster directory is either missing or not owned by the current
  // user.
  else {
    const dataVersion = await readDataDirectoryVersion(dataDir)
    if (dataVersion && dataVersion !== pgDataVersion) {
      throw new Error(
        `PostgreSQL data directory version ${dataVersion} does not match server version ${pgDataVersion}`,
      )
    }
    if (
      !dataVersion ||
      !(await isOwnedByCurrentUser(path.join(dataDir, DATA_DIR)))
    ) {
      await initdb(dataDir)
    }
  }

  const { postgresOptions = '', timeout = 60 } = options

  const dataPath = path.join(dataDir, DATA_DIR)
  let host: string | undefined
  let port: number | undefined
  let listen: PostgresListenOptions

  if (options.host) {
    host = options.host === true ? '127.0.0.1' : options.host
    listen = { type: 'tcp', host, port: options.port }
  } else {
    listen = { type: 'socket', socketDir: dataPath }
  }

  const server = await startPostgresDataDir({
    dataDir: dataPath,
    listen,
    postgresOptions: splitPostgresOptions(postgresOptions),
    log: { filePath: path.join(dataPath, 'postgres.log') },
  })

  if (server.host) {
    port = server.port
  }

  await ensurePostgresDatabase({
    listen: server.listen,
    database: 'test',
  })

  // If a valid timeout is specified, spawn a background process to
  // stop the database when the timeout expires.
  if (timeout > 0) {
    await fs.writeFile(path.join(dataDir, 'stop.log'), '', { flag: 'a' })
    backgroundSpawn(
      'node',
      sift([
        isTest && '--experimental-strip-types',
        new URL(`./stop.${isTest ? 'ts' : 'js'}`, import.meta.url).pathname,
        dataDir,
        host && '--host=' + host,
        port && '--port=' + port,
        '--timeout=' + timeout,
        options.keep && '--keep',
      ]),
    )
  }

  return {
    dsn: port
      ? `postgresql://${host}:${port}/test`
      : `postgresql:///test?host=${encodeURIComponent(dataPath)}`,
    dataDir,
    stop: (options?: StopOptions) => stop(dataDir, { host, port, ...options }),
  }
}

/**
 * Options for `stop()`.
 */
export type StopOptions = {
  /**
   * Preserve the pg-tmp container root after Postgres stops.
   *
   * @default false
   */
  keep?: boolean
  /**
   * Delay (in seconds) before the PostgreSQL instance is stopped. If
   * zero or negative, the instance is stopped even if there are
   * active connections.
   *
   * Note that the instance won't be stopped if active connections
   * exist. In that case, the timeout is restarted and the database
   * continues to run.
   *
   * @default 5
   */
  timeout?: number
  /**
   * Delay in seconds before the first active-connection check.
   *
   * @default 0
   */
  initialTimeout?: number
  /**
   * Stop without waiting for active connections to finish.
   */
  force?: boolean
  /**
   * TCP host used by a server started with `host`.
   */
  host?: string
  /**
   * TCP port used by a server started with `host`.
   */
  port?: number
  /**
   * Retained for compatibility with older pg-tmp releases.
   *
   * @deprecated `stop()` no longer invokes `pg_ctl` directly, so this option
   * has no effect.
   */
  stdio?: StdioOptions
  /**
   * Print lifecycle messages while waiting for connections, stopping Postgres,
   * and removing the container root.
   */
  verbose?: boolean
}

/**
 * Stops a running temporary PostgreSQL server.
 *
 * By default, `stop()` waits for active connections to the `test` database to
 * finish, stops Postgres, and removes the pg-tmp container root.
 *
 * @param dataDir - pg-tmp container root returned by `initdb()` or `start()`.
 * @param options - Stop and cleanup options.
 * @throws If `dataDir/data` is not an initialized PostgreSQL data directory,
 * if idle waiting fails, or if Postgres does not stop before its shutdown
 * timeout.
 *
 * @example
 * ```ts
 * import { stop } from '@pg-nano/pg-tmp'
 *
 * await stop('/tmp/pg_tmp.example', { force: true })
 * ```
 */
export async function stop(dataDir: string, options: StopOptions = {}) {
  dataDir = path.join(dataDir, DATA_DIR)

  if (!(await stat(dataDir))?.isDirectory()) {
    throw new Error('Please specify a valid PostgreSQL data directory')
  }

  const {
    keep,
    timeout = 5,
    initialTimeout = 0,
    host,
    port,
    verbose,
    force,
  } = options

  const listen = resolveListenOptions(dataDir, host, port)
  const logger = verbose ? consoleLogger : undefined

  // If the timeout is set to zero or negative, stop the database even
  // if there are active connections.
  if (!force && timeout > 0) {
    if (verbose) {
      console.log('waiting for active connections to finish')
    }
    for (let attempts = 0; ; attempts++) {
      try {
        await stopPostgresDataDir({
          dataDir,
          listen,
          waitForIdle: {
            database: 'test',
            timeoutMs: (attempts ? timeout : initialTimeout) * 1000,
          },
          logger,
        })
        break
      } catch (error) {
        if (!isIdleTimeout(error)) {
          throw error
        }
      }
    }
  } else {
    if (verbose) {
      console.log('stopping postgres...')
    }
    await stopPostgresDataDir({ dataDir, listen, logger })
  }

  if (!keep) {
    if (verbose) {
      console.log('removing data directory...')
    }
    rmSync(path.dirname(dataDir), {
      maxRetries: 3,
      recursive: true,
      force: true,
    })
  }
}

function getPostgresDataVersion(pgVersion: string) {
  // PG_VERSION stores 9.x clusters as "9.6" and newer clusters as "17".
  return pgVersion.startsWith('9.')
    ? pgVersion.split('.').slice(0, 2).join('.')
    : pgVersion.split('.')[0]
}

async function readDataDirectoryVersion(rootDir: string) {
  return await fs
    .readFile(path.join(rootDir, DATA_DIR, 'PG_VERSION'), 'utf8')
    .then(
      version => version.trim(),
      () => null,
    )
}

async function stat(path: string) {
  return await fs.stat(path).catch(() => null)
}

async function isOwnedByCurrentUser(path: string) {
  if (process.getuid) {
    return (await stat(path))?.uid === process.getuid()
  }
  if (process.platform === 'win32') {
    const username = await exec('cmd', ['/c', 'echo %username%'])
    const owner = await exec('cmd', ['/c', `icacls "${path}"`]).then(
      owner => owner.match(/Owner:\s*(.*)/)?.[1].trim(),
      noop,
    )

    return username === owner
  }
  return false
}

async function exec(cmd: string, args: string[]) {
  const child = spawnChild(cmd, args, { stdio: ['ignore', 'pipe', 'ignore'] })
  let stdout = ''
  child.stdout?.setEncoding('utf8')
  child.stdout?.on('data', chunk => {
    stdout += chunk
  })
  await new Promise<void>((resolve, reject) => {
    child.on('error', reject)
    child.on('exit', code => {
      code === 0 ? resolve() : reject(new Error(`${cmd} exited with ${code}`))
    })
  })
  return stdout.trim()
}

function backgroundSpawn(realCmd: string, realArgs: string[]) {
  let cmd: string
  let argv: string[]
  if (process.platform === 'win32') {
    /** @see https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/start */
    cmd = 'start'
    argv = ['/low']
  } else {
    /** @see https://www.man7.org/linux/man-pages/man1/nice.1.html */
    cmd = 'nice'
    argv = ['-n', '19']
  }
  argv = [...argv, realCmd, ...realArgs]
  spawnChild(cmd, argv, {
    stdio: 'ignore',
    detached: true,
  }).unref()
}

function resolveListenOptions(
  dataDir: string,
  host?: string,
  port?: number,
): PostgresListenOptions {
  return host
    ? { type: 'tcp', host, port }
    : { type: 'socket', socketDir: dataDir }
}

function splitPostgresOptions(options: string) {
  const args: string[] = []
  let current = ''
  let quote: string | undefined
  let escaped = false
  for (const char of options.trim()) {
    if (escaped) {
      current += char
      escaped = false
    } else if (char === '\\') {
      escaped = true
    } else if (quote) {
      if (char === quote) {
        quote = undefined
      } else {
        current += char
      }
    } else if (char === '"' || char === "'") {
      quote = char
    } else if (/\s/.test(char)) {
      if (current) {
        args.push(current)
        current = ''
      }
    } else {
      current += char
    }
  }
  if (escaped) {
    current += '\\'
  }
  if (quote) {
    throw new Error('Unterminated quote in postgresOptions')
  }
  if (current) {
    args.push(current)
  }
  return args
}

function isIdleTimeout(error: unknown) {
  return (
    error instanceof LocalPostgresError &&
    error.message.includes('waiting for Postgres connections to become idle')
  )
}

const consoleLogger: LocalPostgresLogger = {
  info: console.log,
  warn: console.warn,
  error: console.error,
}
