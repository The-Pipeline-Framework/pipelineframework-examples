import {closeSync, fsyncSync, openSync, readFileSync, realpathSync, renameSync, writeSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';

const input = JSON.parse(readFileSync(0, 'utf8'));
async function load(relative) {
  const path = join(input.tree, relative);
  if (realpathSync(path) !== path) throw new Error('Prepared import changed location');
  return import(pathToFileURL(path).href);
}

const kernel = 'node_modules/@tpf/experimental-node-local';
const {createNativeTransitionWorker} = await load(kernel + '/worker.mjs');
const {StructuredFailure} = await load(kernel + '/index.mjs');
const {createApplication} = await load('app/application.js');
const {handlers} = await load('app/business.js');
function writeSynced(path, flags, line) {
  const fd = openSync(path, flags);
  try {
    writeSync(fd, line);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

const counted = Object.fromEntries(Object.entries(handlers).map(([name, handler]) => [
  name, async (record, context) => {
    writeSynced(input.calls, 'a', JSON.stringify({handler: name, value: record.value, context}) + '\n');
    if (name === 'apply' && [11, 12, 13].includes(record.value)) {
      if (record.value === 13) throw new Error('must not be sent to coordinator');
      const retryable = record.value === 11;
      throw new StructuredFailure({code: retryable ? 'TRANSIENT' : 'BUSINESS_REJECTION',
        message: 'must not be sent to coordinator', retryable, owner: 'LOCAL_SHELL', stepId: 'B'});
    }
    const result = await handler(record, context);
    if (name === 'normalize' && input.pauseAfterNormalize) {
      const tmp = input.pauseAfterNormalize + '.tmp';
      writeSynced(tmp, 'w', JSON.stringify({handler: name, value: record.value, context}) + '\n');
      renameSync(tmp, input.pauseAfterNormalize);
      await new Promise(() => {});
    }
    return result;
  },
]));

const target = input.target;
const server = createNativeTransitionWorker({
  bind: runtime => createApplication(runtime, counted),
  expectedProjection: Buffer.from(input.projection, 'base64'),
  tenantId: target.tenantId,
  pipelineId: input.command.pipelineId,
  contractVersion: input.command.contractVersion,
  releaseVersion: target.releaseVersion,
  artifactId: target.runnableArtifactId,
  artifactDigest: target.runnableArtifactDigest,
  profiles: target.profiles,
  sharedSecret: input.secret,
  key: readFileSync(input.key),
  cert: readFileSync(input.cert),
});
server.listen(input.port, '127.0.0.1', () => console.log('READY'));
