import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { connectLogFile } from './logfile.ts'
import { initdb, PREFIX } from './mod.ts'

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), PREFIX))

connectLogFile(path.join(dataDir, 'initdb.log'))

await initdb(dataDir, { stdio: 'inherit' })
