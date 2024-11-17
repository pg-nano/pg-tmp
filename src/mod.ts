import type { StdioOptions } from 'node:child_process'
import { promises as fs, rmSync } from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { dedent, sift, sleep, tryit } from 'radashi'
import spawn from 'tinyspawn'

const OS_TMP = os.tmpdir()

/**
 * Data directories created by this module are named with this prefix.
 */
export const PREFIX = 'pg_tmp.'

export async function initdb(
  dataDir?: string | null,
  { stdio }: { stdio?: StdioOptions } = {},
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

export async function start({
  dataDir,
  host = false,
  port,
  timeout = 60,
  keep,
  postgresOptions = '',
}: StartOptions = {}) {
  const pgVersion = await getPostgresVersion()

  if (!dataDir) {
    // Look for an existing pg_tmp.* directory that was optimistically
    // initialized by a previous `start` call.
    for await (let dir of fs.glob(PREFIX + '*', { cwd: OS_TMP })) {
      dir = path.join(OS_TMP, dir)

      // Postgres versions must match.
      if ((await stat(path.join(dir, pgVersion)))?.isDirectory()) {
        // The 'NEW' file must exist and be owned by the current user.
        const unusedMarker = path.join(dir, 'NEW')
        if (await isOwnedByCurrentUser(unusedMarker)) {
          await fs.rm(unusedMarker)

          dataDir = dir
          break
        }
      }
    }

    // Create a new data directory if none was found.
    if (!dataDir) {
      dataDir = await initdb()
      await fs.rm(path.join(dataDir, 'NEW'))
    }

    // Optimistically initialize another database to speed up future calls.
    backgroundSpawn(
      'node',
      sift([
        !!process.env.TEST && '--experimental-strip-types',
        new URL(`./initdb.${process.env.TEST ? 'ts' : 'js'}`, import.meta.url)
          .pathname,
      ]),
    )
  }
  // If a data directory was provided: Initialize the database if a
  // subdirectory for the current Postgres version is either missing
  // or not owned by the current user.
  else if (!(await isOwnedByCurrentUser(path.join(dataDir, pgVersion)))) {
    await initdb(dataDir)
  }

  if (host) {
    if (host === true) {
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
        !!process.env.TEST && '--experimental-strip-types',
        new URL(`./stop.${process.env.TEST ? 'ts' : 'js'}`, import.meta.url)
          .pathname,
        dataDir,
        host && '--host=' + host,
        port && '--port=' + port,
        '--timeout=' + timeout,
        keep && '--keep',
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
    } catch (error) {
      if (++i < 5) {
        await sleep(100)
      } else {
        throw error
      }
    }
  }

  return port
    ? `postgresql://${host}:${port}/test`
    : `postgresql:///test?host=${encodeURIComponent(dataDir)}`
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

export async function stop(
  dataDir: string,
  { keep, timeout = 5, host, port, stdio, verbose }: StopOptions = {},
) {
  const pgVersion = await getPostgresVersion()
  dataDir = path.join(dataDir, pgVersion)

  if (!(await stat(dataDir))?.isDirectory()) {
    throw new Error('Please specify a valid PostgreSQL data directory')
  }

  const env = {
    ...process.env,
    PGHOST: host ?? dataDir,
    PGPORT: String(port ?? ''),
  }

  // If the timeout is set to zero or negative, stop the database even
  // if there are active connections.
  if (timeout > 0) {
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
    for (let count = 2; count >= 2; ) {
      await sleep(timeout * 1000)
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
    const owner = (await exec('cmd', ['/c', `icacls "${path}"`]))
      .match(/Owner:\s*(.*)/)?.[1]
      .trim()

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
