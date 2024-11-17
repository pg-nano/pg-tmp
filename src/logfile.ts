import fs from 'node:fs'
import { isFunction } from 'radashi'

export function connectLogFile(filename: string) {
  const logFile = fs.createWriteStream(filename)
  process.stdout.write = writeToLogFile(logFile, process.stdout.write)
  process.stderr.write = writeToLogFile(logFile, process.stderr.write)
  process.on('uncaughtException', err => {
    logFile.write(`${err.stack || err.message}\n`)
  })
}

function writeToLogFile(
  logFile: fs.WriteStream,
  write: typeof process.stdout.write,
) {
  return function (
    this: any,
    chunk: string | Uint8Array,
    encoding?: BufferEncoding | ((err?: Error) => void),
    callback?: (err?: Error) => void,
  ) {
    if (!encoding || isFunction(encoding)) {
      callback = encoding
      logFile.write(chunk, callback && mapNullToUndefined(callback))
      return write.call(this, chunk, callback as any)
    }
    logFile.write(chunk, encoding, callback && mapNullToUndefined(callback))
    return write.call(this, chunk, encoding, callback)
  }
}

function mapNullToUndefined<T>(callback: (err?: T) => void) {
  return (err?: T | null) => callback(err ?? undefined)
}
