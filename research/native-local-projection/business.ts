/** Authored business functions: typed transformations; retry/context policy stays in the test shell. */
import type { ProbeValue, ProbeContext, HandlerRegistry } from './generated/models.ts';

export async function a(input: ProbeValue, _context: ProbeContext): Promise<ProbeValue> {
  await Promise.resolve();
  return {value: input.value + 1};
}
export async function b(input: ProbeValue, _context: ProbeContext): Promise<ProbeValue> {
  await Promise.resolve();
  return {value: input.value * 2};
}
export async function c(input: ProbeValue, _context: ProbeContext): Promise<ProbeValue> {
  await Promise.resolve();
  return {value: input.value - 3};
}
export const handlers: HandlerRegistry = {a, b, c};
