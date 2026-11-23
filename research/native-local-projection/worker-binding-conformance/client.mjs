#!/usr/bin/env node
/**
 * Standalone zero-JVM Node.js client for the packaged hosted native coordinator.
 *
 * Commands:
 *   submit <baseUrl> <token> <tenant> <pipeline> <clientKey> <contractVersion> <releaseVersion> <inputType> <value>
 *   lookup <baseUrl> <token> <tenant> <pipeline> <clientKey>
 *   wait   <baseUrl> <token> <tenant> <executionId> [timeoutSeconds]
 */

import process from 'node:process';

async function request(method, url, token, body) {
  const headers = {'Authorization': `Bearer ${token}`};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const response = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} ${url} failed with HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

const [command, baseUrl, token, ...rest] = process.argv.slice(2);
if (!command || !baseUrl || !token) {
  console.error('Usage: client.mjs <submit|lookup|wait> <baseUrl> <token> ...');
  process.exit(2);
}
const root = baseUrl.replace(/\/$/, '');

if (command === 'submit') {
  const [tenant, pipeline, clientKey, contractVersion, releaseVersion, inputType, rawValue] = rest;
  const value = Number.parseInt(rawValue, 10);
  if (!Number.isInteger(value)) throw new Error(`Expected integer value, got ${rawValue}`);
  const payload = {
    clientKey,
    contractVersion,
    releaseVersion,
    inputShape: 'UNI',
    outputStreaming: false,
    inputPayload: {
      payloadTypeId: inputType,
      payloadEncoding: 'tpf-polyglot-int32-json/1',
      payload: JSON.stringify({value}),
    },
  };
  const receipt = await request(
    'POST',
    `${root}/tpf/control-plane/tenants/${tenant}/pipelines/${pipeline}/execution-admissions`,
    token,
    payload,
  );
  console.log(JSON.stringify(receipt));
} else if (command === 'lookup') {
  const [tenant, pipeline, clientKey] = rest;
  const receipt = await request(
    'GET',
    `${root}/tpf/control-plane/tenants/${tenant}/pipelines/${pipeline}/execution-admissions/${clientKey}`,
    token,
  );
  console.log(JSON.stringify(receipt));
} else if (command === 'wait') {
  const [tenant, executionId, rawTimeout = '30'] = rest;
  const deadline = Date.now() + Number(rawTimeout) * 1000;
  const statusUrl = `${root}/tpf/control-plane/tenants/${tenant}/executions/${executionId}`;
  let status = await request('GET', statusUrl, token);
  while (['QUEUED', 'RUNNING', 'WAIT_RETRY'].includes(status.status) && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 100));
    status = await request('GET', statusUrl, token);
  }
  const result = await request('GET', `${statusUrl}/result`, token);
  console.log(JSON.stringify({status, result}));
} else {
  console.error(`Unknown command: ${command}`);
  process.exit(2);
}
