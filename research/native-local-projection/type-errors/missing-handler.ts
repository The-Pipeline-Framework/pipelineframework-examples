import type { HandlerRegistry } from '../generated/models.ts';
const identity = async (value: {value: number}) => value;
export const handlers: HandlerRegistry = {a: identity, b: identity};
