import { localApplication } from './src/application.ts';

const value = Number(process.argv[2] ?? '3');
const application = localApplication({onObservation: event => console.error(JSON.stringify(event))});
const result = await application.run({value}, {executionId: 'local-example', traceId: 'local-trace'});
console.log(JSON.stringify(result));
if (result.kind !== 'COMPLETED') process.exitCode = 1;
