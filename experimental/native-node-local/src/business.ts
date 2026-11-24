import type { HandlerRegistry } from '../generated/project/application.ts';

export const normalize: HandlerRegistry['normalize'] = ({value}) => ({
  kind: 'VALUE',
  value: {value: value + 1},
});

export const apply: HandlerRegistry['apply'] = ({value}) => ({
  kind: 'VALUE',
  value: {value: value * 2},
});

export const finish: HandlerRegistry['finish'] = ({value}) => ({
  kind: 'VALUE',
  value: {value: value - 3},
});

export const handlers = {normalize, apply, finish} satisfies HandlerRegistry;
