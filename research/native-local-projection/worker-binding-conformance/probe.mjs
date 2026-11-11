import {readFileSync} from 'node:fs';
import {NodeLocalDiagnostic, PolyglotInt32PayloadBinding} from '@tpf/experimental-node-local';

const cases = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const results = cases.map(test => {
  let observed;
  try {
    const binding = new PolyglotInt32PayloadBinding(...test.typeIds);
    switch (test.operation) {
      case 'catalog':
        observed = {accepted: true};
        break;
      case 'encode':
        observed = binding.encode(test.typeId, test.value);
        break;
      case 'decode': {
        const value = binding.decode(test.typeId, test.envelope);
        if (!Number.isInteger(value) || Object.is(value, -0)) {
          throw new Error(`Unexpected decoded int32 in ${test.id}`);
        }
        observed = {value, canonical: binding.encode(test.typeId, value).payload};
        break;
      }
      default:
        throw new Error(`Unexpected operation in ${test.id}`);
    }
  } catch (error) {
    if (!(error instanceof NodeLocalDiagnostic)) {
      throw error;
    }
    observed = {error: error.code};
  }
  return {id: test.id, observed};
});
console.log(JSON.stringify(results));
