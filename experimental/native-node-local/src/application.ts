import { createNodeLocalRuntime } from '@tpf/experimental-node-local';
import { createApplication } from '../generated/project/application.ts';
import { handlers } from './business.ts';

export function localApplication(options?: Parameters<typeof createNodeLocalRuntime>[0]) {
  return createApplication(createNodeLocalRuntime(options), handlers);
}
