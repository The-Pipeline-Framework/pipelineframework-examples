import type { PresenceProbe } from './generated/models.ts';
export const absent: PresenceProbe = {requiredValue: 0, nullableValue: null};
export const present: PresenceProbe = {requiredValue: 0, nullableValue: 1, optionalValue: 0, optionalNullableValue: null};
