import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
export const profile = 'tpf-polyglot-unary-policy/1';

export function verifyGenerated(directory, receiptSha256) {
  const read = name => readFileSync(new URL(name, directory));
  const receiptBytes = read('generation-receipt.json');
  if (receiptSha256) assert.equal(sha256(receiptBytes), receiptSha256, 'generation receipt digest');
  const receipt = JSON.parse(receiptBytes);
  assert.equal(receipt.schemaVersion, 1);
  assert.equal(receipt.status, 'success');
  assert.equal(receipt.profile, profile);
  assert.deepEqual(Object.keys(receipt.files).sort(), ['application.ts', 'diagnostics.txt', 'models.ts', 'projection.json']);
  for (const [name, digest] of Object.entries(receipt.files)) {
    assert.equal(sha256(read(name)), digest, `Generated file digest mismatch: ${name}`);
  }
  const projection = JSON.parse(read('projection.json'));
  assert.equal(projection.profile, profile);
  assert.equal(projection.kind, 'compiler-node-local');
  assert.equal(projection.provenance.dslSha256, receipt.source.inputs[0].sha256);
  assert.equal(projection.provenance.bindingsSha256, receipt.source.inputs[1].sha256);
  return {receipt, projection};
}

if (import.meta.main) {
  try {
    const {projection} = verifyGenerated(new URL('./generated/project/', import.meta.url));
    for (const [file, digest] of Object.entries({
      'pipeline.yaml': projection.provenance.dslSha256,
      'native-bindings.json': projection.provenance.bindingsSha256,
    })) {
      assert.equal(sha256(readFileSync(new URL(file, import.meta.url))), digest,
        `Generated output does not match the current ${file}`);
    }
  } catch (error) {
    console.error(`Cannot use generated/project: ${error.message}. Generate the current committed project as described in README.md.`);
    process.exitCode = 1;
  }
}
