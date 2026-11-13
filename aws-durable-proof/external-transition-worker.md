# Cross-account transition Lambda proof

Use the AWS Durable coordination host and a short, deterministic typed pipeline.
The coordinator, Action and Work Lambdas, execution stores, and transition queues
belong to the coordinator account. The second account owns the transition Lambda.
The same signed SQS messages and CloudFormation deployment contract apply to any
deployment of the OSS AWS Durable host.

## Inputs

- Use a fresh, isolated coordinator scope and a single outstanding execution. Use the
  reference runtime template `aws-durable-queue-async.yaml` from the selected runtime
  candidate. Start with a same-account `FUNCTION` control run. For the external run,
  use a separate fresh hardened stack with `TransitionWorkerPlacement=COMPUTE` and
  the template's `ComputeTransitionConsumerValidation` attestation. Verify that only
  the external mapping consumes the request queue. Here `COMPUTE` is the template's
  external-consumer selection. Keep the original compiled contract and release.
- Use the original compiled app classes and metadata in both deployments. The
  runtime release plugin's `hosted-aws-durable-echo` fixture is a suitable one-step
  app. Its ordinary Quarkus build produces `target/function.zip` and an immutable
  release descriptor with a typed canary. Build with the same selected runtime
  candidate in both accounts, including the transition-only host option.
- Supply the existing queue pair and request DLQ from the coordinator deployment.
  Set the request visibility to at least six times the worker's 60-second timeout
  with no batching window. Keep the command, app execution and response within the
  configured coordinator reply deadline and signing tolerance.
- Supply an approved second-account Lambda role, a versioned ZIP in that account
  and Region, and a version-pinned local copy of the coordinator's worker signing
  key. The execution role needs the request receive/delete/attributes and response
  send permissions, the applicable queue encryption permissions, and workload
  logging. The deployment identity resolves the versioned signing secret. The
  worker role needs no access to the coordinator's semantic stores, Work queue,
  callback functions, or resume-token secret. Do not copy that resume secret.

## Run and record

1. In the isolated same-account control, submit a fresh client key and the release's
   typed canary input to the ordinary AWS Durable ingress. Record the execution
   pin and typed result through the existing Action gateway. Use a fresh client
   key in the external run. The runtime's release-plugin Echo fixture documents
   the typed input and expected output in the generated release descriptor.
2. Confirm the isolated external coordinator selects the fixed SQS request/response
   pair and has no local fallback or competing consumer. In the second account,
   apply [the worker template](external-transition-worker.yaml) using the supplied
   inputs. Set a fresh `PublicationId` for each code or configuration revision.
   Check the numbered Lambda ARN, actual code hash and enabled mapping against
   the exact request queue ARN before submitting.
3. Submit the canary to the external coordinator ingress. Collect the execution ID,
   release/contract pin, worker invocation and the single authoritative result.
   Verify the typed result matches the control and the business step ran in the
   second-account Lambda. Record only the necessary identifiers and outcome in
   the proof receipt. Keep signing keys and raw execution payloads out of logs.
4. With fresh test keys, check that a bad request signature cannot execute a step,
   and a wrong compiled release cannot succeed. Interrupt or temporarily deny one
   reply publication in the test scope and verify the existing retry or recorded
   failure path. Allow repeated physical execution of this pure step. Require one
   authoritative terminal result for the execution.
5. Disable the customer mapping and remove both disposable proof scopes according
   to their owners' cleanup rules. Retain the numbered version and queue only if
   unfinished work still needs them.

The template contains only the second-account worker, its numbered version and
the event source mapping. Deployment inputs and live evidence are supplied by the
operator. The example does not run an AWS deployment during Maven verification.

[AWS cross-account SQS event source example](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs-cross-account-example.html)
