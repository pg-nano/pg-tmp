import type { StdioOptions } from 'node:child_process'
import { promises as fs, rmSync } from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { dedent, noop, sift, sleep, tryit } from 'radashi'
import { glob } from 'tinyglobby'
import spawn from 'tinyspawn'

const OS_TMP = os.tmpdir()
const isTest = !!process.env.PGTMP_TEST

/**
 * Data directories created by this module are named with this prefix.
 */
export const PREFIX = 'pg_tmp.'

export type InitOptions = {
  /**
   * Control the I/O streams of the `initdb` command. The only useful
   * value is `'inherit'`, which forwards the I/O streams to the
   * parent process (for debugging purposes mainly).
   */
  stdio?: StdioOptions
}

/**
 * Initializes a new PostgreSQL data directory.
 *
 * This function sets up the necessary file structure and
 * configuration for a new PostgreSQL instance. It creates a data
 * directory (or uses an existing one if provided), configures it for
 * optimal performance in temporary environments (e.g. disabling
 * fsync), and prepares it for starting a PostgreSQL server.
 *
 * @param dataDir - Optional. The path to the directory where
 *   PostgreSQL data will be stored. If `null` or unspecified, a new
 *   temporary directory will be created automatically with the prefix
 *   `"pg_tmp."`.
 * @returns A promise that resolves to the path of the initialized
 * data directory.
 */
export async function initdb(
  dataDir?: string | null,
  { stdio }: InitOptions = {},
) {
  dataDir ||= await fs.mkdtemp(path.join(OS_TMP, PREFIX))

  const pgVersion = await getPostgresVersion()

  await fs.mkdir(path.join(dataDir, pgVersion), { recursive: true })
  await spawn(
    'initdb',
    [
      '--nosync',
      '-D',
      path.join(dataDir, pgVersion),
      '-E',
      'UNICODE',
      '-A',
      'trust',
    ],
    { stdio },
  )

  const confPath = path.join(dataDir, pgVersion, 'postgresql.conf')
  await fs.appendFile(
    confPath,
    dedent`
      unix_socket_directories = '${path.join(dataDir, pgVersion)}'
      listen_addresses = ''
      shared_buffers = 12MB
      fsync = off
      synchronous_commit = off
      full_page_writes = off
      log_min_duration_statement = 0
      log_connections = on
      log_disconnections = on
    `,
  )

  await fs.writeFile(path.join(dataDir, 'NEW'), '')
  return dataDir
}

export type StartOptions = {
  /**
   * Where the PostgreSQL data directory is located. If unspecified, a
   * new directory is created.
   */
  dataDir?: string
  /**
   * If true, the PostgreSQL instance will listen on `127.0.0.1`. If
   * false, a Unix socket is used at the root of the data directory.
   * You may also specify a custom host address.
   *
   * @default false
   */
  host?: string | boolean
  /**
   * The port to listen on. If unspecified, an unused port is selected
   * automatically.
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
   * If true, the data directory won't be removed when the PostgreSQL
   * instance is stopped.
   */
  keep?: boolean
  /**
   * Options passed directly to the `postgres` command.
   *
   * Note that `listen_addresses` and `port` are already set for you.
   */
  postgresOptions?: string
}

/**
 * The object returned by the `start` function.
 */
export type PgTmp = Awaited<ReturnType<typeof start>>

/**
 * Starts a PostgreSQL server instance.
 *
 * This function handles the creation or reuse of a data directory,
 * starts the `postgres` process, and ensures a test database is
 * available. It can manage the server's lifecycle with an automatic
 * shutdown timeout and provides options for network configuration
 * (host and port).
 *
 * You are not required to call `initdb` before calling `start`. If
 * you don't, a new data directory will be created automatically.
 *
 * @returns A promise that resolves to the DSN (Data Source Name)
 *   string for connecting to the 'test' database. The DSN format will
 *   be `postgresql://{host}:{port}/test` if a host and port are used,
 *   or `postgresql:///test?host={dataDir}` if a Unix socket is used.
 */
export async function start(options: StartOptions = {}) {
  const pgVersion = await getPostgresVersion()

  let { dataDir } = options

  if (!dataDir) {
    // Look for an existing pg_tmp.* directory that was optimistically
    // initialized by a previous `start` call.
    for (let dir of await glob(PREFIX + '*', { cwd: OS_TMP })) {
      dir = path.join(OS_TMP, dir)

      // Postgres versions must match.
      if ((await stat(path.join(dir, pgVersion)))?.isDirectory()) {
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
  // subdirectory for the current Postgres version is either missing
  // or not owned by the current user.
  else if (!(await isOwnedByCurrentUser(path.join(dataDir, pgVersion)))) {
    await initdb(dataDir)
  }

  let { postgresOptions = '', timeout = 60 } = options

  let host: string | undefined
  let port: number | undefined

  if (options.host) {
    if (options.host === true) {
      host = '127.0.0.1'
    }
    port ??= await getUnusedPort()
    postgresOptions &&= postgresOptions + ' '
    postgresOptions += `-c listen_addresses='*' -c port=${port}`
  }

  // If a valid timeout is specified, spawn a background process to
  // stop the database when the timeout expires.
  if (timeout > 0) {
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

  dataDir = path.join(dataDir, pgVersion)

  const startFlags = [
    '-W', // Don't wait for confirmation
    '-s', // Silent mode
    '-D', // Data directory
    dataDir,
    '-l', // Log file
    path.join(dataDir, 'postgres.log'),
  ]

  if (postgresOptions) {
    startFlags.push('-o', postgresOptions)
  }

  await spawn('pg_ctl', [...startFlags, 'start'])

  for (let i = 0; i < 5; ) {
    try {
      await spawn('createdb', ['-E', 'UNICODE', 'test'], {
        env: {
          ...process.env,
          PGHOST: dataDir,
          PGPORT: String(port ?? ''),
        },
      })
      break
    } catch (error: any) {
      if (error.message.includes('already exists')) {
        break
      }
      if (++i < 5) {
        await sleep(100)
      } else {
        throw error
      }
    }
  }

  return {
    dsn: port
      ? `postgresql://${host}:${port}/test`
      : `postgresql:///test?host=${encodeURIComponent(dataDir)}`,
    dataDir,
    stop: (options?: StopOptions) => stop(dataDir, { host, port, ...options }),
  }
}

export type StopOptions = {
  /**
   * If true, the data directory won't be removed when the PostgreSQL
   * instance is stopped.
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
   * The first timeout before the database is checked for active
   * connections.
   *
   * @default 0
   */
  initialTimeout?: number
  /**
   * If true, the database is forcibly stopped even if there are
   * active connections.
   */
  force?: boolean
  /**
   * The host to connect to.
   */
  host?: string
  /**
   * The port to connect to.
   */
  port?: number
  /**
   * Control the I/O streams of the `pg_ctl stop` command. The only
   * useful value is `'inherit'`, which forwards the I/O streams to
   * the parent process (for debugging purposes mainly).
   */
  stdio?: StdioOptions
  /**
   * Enable verbose logging.
   */
  verbose?: boolean
}

/**
 * Stops a running PostgreSQL server instance and optionally cleans up
 * its data directory.
 *
 * This function gracefully stops the PostgreSQL server associated
 * with the given data directory. It can wait for active connections
 * to close before shutting down and can remove the data directory
 * unless specified otherwise.
 *
 * @param dataDir - The root path of the PostgreSQL data directory
 * (the one created by `initdb` or `start`, not the versioned
 * subdirectory).
 * @returns A promise that resolves when the server has been stopped
 * and cleanup (if any) is complete.
 * @throws Will throw an error if the specified `dataDir` is not a
 * valid PostgreSQL data directory.
 */
export async function stop(dataDir: string, options: StopOptions = {}) {
  const pgVersion = await getPostgresVersion()
  dataDir = path.join(dataDir, pgVersion)

  if (!(await stat(dataDir))?.isDirectory()) {
    throw new Error('Please specify a valid PostgreSQL data directory')
  }

  const {
    keep,
    timeout = 5,
    initialTimeout = 0,
    host,
    port,
    stdio,
    verbose,
    force,
  } = options

  const env = {
    ...process.env,
    PGHOST: host ?? dataDir,
    PGPORT: String(port ?? ''),
  }

  // If the timeout is set to zero or negative, stop the database even
  // if there are active connections.
  if (!force && timeout > 0) {
    if (verbose) {
      console.log('waiting for active connections to finish')
    }
    // Wait for all active PostgreSQL connections to finish before
    // stopping the server. This query checks for any active database
    // connections and loops until they're all closed.
    const testQuery = /* sql */ `
      SELECT count(*) FROM pg_stat_activity
      WHERE datname IS NOT NULL
      AND state IS NOT NULL;
    `
    for (let count = 2, attempts = 0; count >= 2; ) {
      await sleep((attempts++ ? timeout : initialTimeout) * 1000)
      const [error, result] = await tryit(spawn)(
        'psql',
        ['test', '--no-psqlrc', '-At', '-c', testQuery],
        { env },
      )
      if (error) {
        if ('stderr' in error) {
          console.error(error.stderr)
        }
        throw error
      }
      count = Number(result.stdout.trim() || 0)
      if (verbose) {
        console.log(`number of active connections: ${count}`)
      }
    }
  }

  if (verbose) {
    console.log('stopping postgres...')
  }

  // Stop the server.
  await spawn('pg_ctl', ['-W', '-D', dataDir, 'stop'], { env, stdio })

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

async function getUnusedPort() {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer()
    server.listen(0, '0.0.0.0', () => {
      const addr = server.address()
      if (!addr || typeof addr !== 'object') {
        return reject(new Error('Failed to get unused port'))
      }
      const port = addr.port
      server.close(() => resolve(port))
    })
    server.on('error', reject)
  })
}

async function getPostgresVersion() {
  return (await spawn('pg_ctl', ['-V'])).stdout.split(' ')[2]
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
  return (await spawn(cmd, args)).stdout.trim()
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
  spawn(cmd, argv, {
    stdio: 'ignore',
    detached: true,
  }).unref()
}
