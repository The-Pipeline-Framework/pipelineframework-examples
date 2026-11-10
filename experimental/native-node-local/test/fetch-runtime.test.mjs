import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fetchRuntime } from '../fetch-runtime.mjs';
import { sha256 } from '../verify-project.mjs';

const repository = 'The-Pipeline-Framework/pipelineframework-runtime';
const packageFile = 'tpf-experimental-node-local-0.0.0.tgz';
const bytes = Buffer.from('candidate bytes');
const pin = {run: '123456', head: 'a'.repeat(40), tarballSha256: sha256(bytes)};

function fakeGh({head = pin.head, download = 'success'} = {}) {
  return args => {
    if (args[0] === '--version') return 'gh version 2.100.0';
    if (args[0] === 'api') {
      assert.deepEqual(args, ['api', '--hostname', 'github.com', `repos/${repository}/actions/runs/${pin.run}`]);
      return JSON.stringify({
        id: Number(pin.run), head_sha: head, head_repository: {full_name: repository},
        path: '.github/workflows/experimental-node-local.yml', conclusion: 'success',
      });
    }
    const directory = args.at(-1);
    assert.deepEqual(args.slice(0, -1), ['run', 'download', pin.run, '--repo', `github.com/${repository}`,
      '--name', `node-local-runtime-${pin.head}`, '--dir']);
    if (download === 'expired') throw new Error('artifact expired');
    if (download === 'symlink') symlinkSync('SHA256SUMS', join(directory, packageFile));
    else writeFileSync(join(directory, packageFile), download === 'tampered' ? 'altered' : bytes);
    writeFileSync(join(directory, 'SHA256SUMS'), `${pin.tarballSha256}  ./${packageFile}\n`);
  };
}

function temporaryDirectory(test) {
  const directory = mkdtempSync(join(tmpdir(), 'tpf-runtime-download-'));
  test.after(() => rmSync(directory, {recursive: true, force: true}));
  return directory;
}

test('makes only the matching workflow tarball available for install', test => {
  const directory = temporaryDirectory(test);
  const tarball = fetchRuntime(pin, directory, fakeGh());
  assert.equal(tarball, join(directory, 'runtime', packageFile));
  assert.deepEqual(readFileSync(tarball), bytes);
});

for (const [name, options, diagnostic] of [
  ['another runtime head', {head: 'b'.repeat(40)}, /does not match the requested successful workflow/],
  ['tampered bytes', {download: 'tampered'}, /does not match the pinned SHA-256/],
  ['an expired artifact', {download: 'expired'}, /artifact expired/],
  ['an artifact symlink', {download: 'symlink'}, /as regular files/],
]) {
  test(`rejects ${name} without staging a package`, test => {
    const directory = temporaryDirectory(test);
    assert.throws(() => fetchRuntime(pin, directory, fakeGh(options)), diagnostic);
    assert.equal(existsSync(join(directory, 'runtime')), false);
    assert.deepEqual(readdirSync(directory), []);
  });
}
