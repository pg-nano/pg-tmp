import mri from 'mri'
import path from 'node:path'
import { shake } from 'radashi'
import { connectLogFile } from './logfile.ts'
import { stop } from './mod.ts'

const {
  _: [dataDir],
  host,
  port,
  keep,
  timeout,
} = mri(process.argv.slice(2))

connectLogFile(path.join(dataDir, 'stop.log'))

const options = shake({
  host,
  port,
  keep,
  timeout,
})

console.log('options =>', options)

await stop(dataDir, {
  ...options,
  stdio: 'inherit',
  verbose: true,
})
