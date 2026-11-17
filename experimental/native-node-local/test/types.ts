import type { HandlerRegistry } from '../generated/project/application.ts';

const incompatible: HandlerRegistry = {
  normalize: ({value}) => ({kind: 'VALUE', value: {value}}),
  // @ts-expect-error The generated output field is int32, represented as number.
  apply: () => ({kind: 'VALUE', value: {value: 'text'}}),
  finish: ({value}) => ({kind: 'VALUE', value: {value}}),
};
void incompatible;
