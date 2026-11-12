import { execFileSync } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sha256 } from './verify-project.mjs';

const repository = 'The-Pipeline-Framework/pipelineframework-runtime';
const workflow = '.github/workflows/experimental-node-local.yml';
const packageFile = 'tpf-experimental-node-local-0.0.0.tgz';

export function fetchRuntime({run, head, tarballSha256}, generated, gh = args => execFileSync('gh', args, {encoding: 'utf8'})) {
  if (!/^\d+$/.test(run) || !/^[0-9a-f]{40}$/.test(head) || !/^[0-9a-f]{64}$/.test(tarballSha256)) {
    throw new Error('Runtime candidate needs an exact workflow run, head SHA and tarball SHA-256');
  }
  const destination = join(generated, 'runtime');
  if (existsSync(destination)) throw new Error('Move the existing generated/runtime directory aside before downloading');
  const [, major, minor] = /^gh version (\d+)\.(\d+)/.exec(gh(['--version'])) ?? [];
  if (!(Number(major) > 2 || (Number(major) === 2 && Number(minor) >= 100))) {
    throw new Error('Install GitHub CLI 2.100 or newer');
  }
  const metadata = JSON.parse(gh(['api', '--hostname', 'github.com', `repos/${repository}/actions/runs/${run}`]));
  if (String(metadata.id) !== run || metadata.head_sha !== head ||
    metadata.path?.split('@', 1)[0] !== workflow ||
    metadata.head_repository?.full_name?.toLowerCase() !== repository.toLowerCase() ||
    metadata.conclusion !== 'success') {
    throw new Error('Runtime run does not match the requested successful workflow and source commit');
  }

  mkdirSync(generated, {recursive: true});
  const staging = mkdtempSync(join(generated, 'runtime-'));
  try {
    gh(['run', 'download', run, '--repo', `github.com/${repository}`, '--name', `node-local-runtime-${head}`, '--dir', staging]);
    const files = readdirSync(staging);
    if (files.length !== 2 || !files.includes(packageFile) || !files.includes('SHA256SUMS') ||
      !files.every(file => lstatSync(join(staging, file)).isFile())) {
      throw new Error('Expected exactly the runtime tarball and SHA256SUMS as regular files');
    }
    const digest = sha256(readFileSync(join(staging, packageFile)));
    const checksum = readFileSync(join(staging, 'SHA256SUMS'), 'utf8').trim();
    if (digest !== tarballSha256 || checksum !== `${digest}  ./${packageFile}`) {
      throw new Error('Downloaded runtime tarball does not match the pinned SHA-256 and workflow checksum');
    }
    renameSync(staging, destination);
    return join(destination, packageFile);
  } finally {
    rmSync(staging, {recursive: true, force: true});
  }
}

if (import.meta.main) {
  try {
    const pin = JSON.parse(readFileSync(new URL('./runtime-candidate.json', import.meta.url)));
    const path = fetchRuntime(pin, fileURLToPath(new URL('./generated/', import.meta.url)));
    console.log(`Verified ${path} from ${repository}@${pin.head} (run ${pin.run}, SHA-256 ${pin.tarballSha256})`);
  } catch (error) {
    console.error(`Cannot retrieve the pinned runtime candidate. Check gh authentication, Actions access, and artifact expiry. ${error.message}`);
    process.exitCode = 1;
  }
}
