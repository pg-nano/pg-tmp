import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { sleep } from 'radashi'
import spawn from 'tinyspawn'
import { initdb, PREFIX, start } from '../src/mod.ts'

const pgVersion = await getPostgresVersion()

test('full lifecycle', async () => {
  const dataDir = await initdb()
  const { dsn, stop } = await start({
    timeout: 0,
    dataDir,
  }).catch(error => {
    console.error(error.stderr)
    throw error
  })
  expect(typeof dsn).toBe('string')
  await verifyDatabase(dsn)
  await stop()
})

test('automatic deletion', async () => {
  const dataDir = await initdb()
  const { dsn } = await start({
    timeout: 0.5,
    dataDir,
  })

  // Verify the database is accessible.
  await verifyDatabase(dsn)

  // Buffer the stop.log file for debugging.
  const logfile = tail(path.join(dataDir, 'stop.log'), 100)

  // Print the stop.log file content before the test timeout.
  sleep(3000).then(() => {
    if (!logfile.closed) {
      console.error(logfile.content)
      logfile.close()
    }
  })

  // Wait for the data directory to be automatically removed.
  while (true) {
    await sleep(100)
    if (!fs.existsSync(dataDir)) {
      break
    }
  }

  logfile.close()
})

describe('initdb', () => {
  test('with null data directory', async () => {
    const dataDir = await initdb(null)
    expect(dataDir).toBeDefined()
    verifyDataDirectory(dataDir)
  })

  test('pass a new data directory', async () => {
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pg_tmp.test.'))
    const result = await initdb(dataDir)
    expect(result).toBe(dataDir)
    verifyDataDirectory(dataDir)
  })

  test('pass an initialized data directory', async () => {
    const dataDir = await initdb()
    const promise = initdb(dataDir)
    const stderrPromise = promise.catch(error =>
      error.stderr
        .replaceAll(os.tmpdir(), '$TMPDIR')
        .replace(/pg_tmp\.(\w+)/g, 'pg_tmp.XXXXXX'),
    )
    await expect(promise).rejects.toThrow()
    await expect(stderrPromise).resolves.toMatchInlineSnapshot(`
      "initdb: error: directory "$TMPDIR/pg_tmp.XXXXXX/17.0" exists but is not empty
      initdb: hint: If you want to create a new database system, either remove or empty the directory "$TMPDIR/pg_tmp.XXXXXX/17.0" or run initdb with an argument other than "$TMPDIR/pg_tmp.XXXXXX/17.0"."
    `)
  })
})

// Remove all pg_tmp.* directories created while testing.
afterAll(async () => {
  const cwd = os.tmpdir()
  for (const dir of fs.globSync(PREFIX + '*', { cwd })) {
    fs.rmSync(path.join(cwd, dir), {
      maxRetries: 3,
      recursive: true,
      force: true,
    })
  }
})

async function getPostgresVersion() {
  return (await spawn('pg_ctl', ['-V'])).stdout.split(' ')[2]
}

function verifyDataDirectory(dataDir: string) {
  expect(fs.statSync(dataDir).isDirectory()).toBe(true)
  expect(fs.existsSync(path.join(dataDir, pgVersion, 'postgresql.conf'))).toBe(
    true,
  )
}

async function verifyDatabase(dsn: string) {
  const result = await spawn('psql', [
    '--no-psqlrc',
    '-At',
    '-c',
    'select 1',
    dsn,
  ])
  expect(result.exitCode).toBe(0)
  expect(result.stdout.trim()).toBe('1')
}

function tail(file: string, interval: number) {
  let content = ''
  let closed = false

  const fd = fs.openSync(file, 'r')
  const reader = setInterval(() => {
    const buffer = Buffer.alloc(1024)
    const bytesRead = fs.readSync(fd, buffer, 0, 1024, content.length)
    content += buffer.toString('utf8', 0, bytesRead)
  }, interval)

  return {
    get content() {
      return content
    },
    get closed() {
      return closed
    },
    close() {
      if (!closed) {
        closed = true
        clearInterval(reader)
        fs.closeSync(fd)
      }
    },
  }
}
