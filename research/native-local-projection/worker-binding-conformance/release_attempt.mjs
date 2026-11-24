import {readFileSync, realpathSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';

const request = JSON.parse(readFileSync(0, 'utf8'));
const imports = [];
async function load(relative) {
  const file = join(request.tree, relative);
  if (realpathSync(file) !== file) {
    throw new Error(`Prepared import changed location: ${relative}`);
  }
  const module = await import(pathToFileURL(file).href);
  imports.push(relative);
  return module;
}
const {
  createNodeLocalRuntime,
  NodeLocalDiagnostic,
  PolyglotInt32PayloadBinding,
} = await load('node_modules/@tpf/experimental-node-local/index.mjs');
const {createApplication} = await load('app/application.js');
await load('app/models.js');
const {handlers} = await load('app/business.js');
const calls = [];
const counted = Object.fromEntries(Object.entries(handlers).map(([name, handler]) => [
  name, (input, context) => {
    calls.push({handler: name, value: input.value, context});
    return handler(input, context);
  },
]));
const evidence = {calls, imports};

if (request.checkOrdinaryLoad) {
  createApplication(createNodeLocalRuntime(), counted);
  evidence.ordinaryLoadAccepted = true;
}

try {
  const expectedProjectionJson = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})
    .decode(Buffer.from(request.projection, 'base64'));
  const runtime = createNodeLocalRuntime({expectedProjectionJson});
  const application = createApplication(runtime, counted);
  const codec = new PolyglotInt32PayloadBinding(...request.typeIds);
  const value = codec.decode(request.inputType, request.inputPayload);
  const result = await application.run({value}, request.context);
  if (result.kind !== 'COMPLETED') {
    throw new Error(`Expected COMPLETED, received ${JSON.stringify(result)}`);
  }
  evidence.payload = codec.encode(request.outputType, result.outcome.value.value);
} catch (error) {
  if (!(error instanceof NodeLocalDiagnostic)) {
    throw error;
  }
  evidence.error = error.code;
}

console.log(JSON.stringify(evidence));
