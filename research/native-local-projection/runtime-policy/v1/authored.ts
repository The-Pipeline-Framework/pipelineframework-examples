/** Turn a typed business value into the policy profile's explicit result carrier. */
import { handlers as business } from '../../business.ts';
import type { HandlerRegistry, ProbeContext, ProbeValue } from './generated/baseline/models.ts';
import type { HandlerRegistry as ReorderedRegistry } from './generated/changed-order/models.ts';
import type { HandlerRegistry as RetryRegistry } from './generated/downstream-retry/models.ts';

type ValueResult = Readonly<{kind: 'VALUE'; value: ProbeValue}>;
type PolicyHandler = (input: ProbeValue, context: ProbeContext) => Promise<ValueResult>;

const bind = (handler: HandlerRegistry[keyof HandlerRegistry]): PolicyHandler =>
  async (input, context) => ({kind: 'VALUE', value: await handler(input, context)});

const authored: HandlerRegistry & ReorderedRegistry & RetryRegistry = business;
export const handlers = {a: bind(authored.a), b: bind(authored.b), c: bind(authored.c)};
