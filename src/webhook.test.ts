import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import test from 'node:test';

import { SIGNATURE_HEADER, TIMESTAMP_HEADER } from './auth.ts';
import { handleSendGridWebhook } from './index.ts';

const { privateKey, publicKey } = generateKeyPairSync('ec', {
  namedCurve: 'prime256v1',
});
const publicKeyPem = publicKey.export({ type: 'spki', format: 'pem' });
if (typeof publicKeyPem !== 'string') {
  throw new Error('expected PEM public key');
}

const ENROLLED_KEY = 'client-test-enrolled';
const DUMMY_KEY = 'client-invalid-recheck-2026';
const BODY = JSON.stringify([
  {
    event: 'processed',
    email: 'test@example.com',
    timestamp: 1,
    sg_message_id: 'unit-test',
  },
]);
const TIMESTAMP = '1700000000';

const enrolledEnv = {
  SENDGRID_VERIFICATION_KEYS: JSON.stringify({
    [ENROLLED_KEY]: publicKeyPem,
  }),
};

function signPayload(timestamp: string, body: string): string {
  return sign('sha256', Buffer.from(timestamp + body), privateKey).toString(
    'base64',
  );
}

function forbidNetwork(): typeof fetch {
  return async () => {
    throw new Error('outbound request is not allowed');
  };
}

function post(
  apiKey: string | null,
  headers: Record<string, string> = {},
): Request {
  const url = new URL('https://unit.test/sendgrid');
  if (apiKey != null) {
    url.searchParams.set('apikey', apiKey);
  }
  return new Request(url, {
    method: 'POST',
    headers,
    body: BODY,
  });
}

async function expectUnauthorized(
  request: Request,
  env: { SENDGRID_VERIFICATION_KEYS?: string } = {},
): Promise<void> {
  const response = await handleSendGridWebhook(request, env, forbidNetwork());
  assert.equal(response.status, 401);
  assert.equal(await response.text(), 'Unauthorized');
}

test('rejects a missing apikey', async () => {
  await expectUnauthorized(post(null));
});

test('rejects an empty apikey', async () => {
  await expectUnauthorized(post(''));
});

test('rejects a dummy apikey even when the body is signed for another key', async () => {
  await expectUnauthorized(
    post(DUMMY_KEY, {
      [SIGNATURE_HEADER]: signPayload(TIMESTAMP, BODY),
      [TIMESTAMP_HEADER]: TIMESTAMP,
    }),
    enrolledEnv,
  );
});

test('rejects a missing SendGrid signature', async () => {
  await expectUnauthorized(post(ENROLLED_KEY), enrolledEnv);
});

test('rejects a bogus SendGrid signature', async () => {
  await expectUnauthorized(
    post(ENROLLED_KEY, {
      [SIGNATURE_HEADER]: 'bogus-sig-recheck',
      [TIMESTAMP_HEADER]: TIMESTAMP,
    }),
    enrolledEnv,
  );
});

test('rejects an enrolled key the events API does not accept', async () => {
  let calls = 0;
  const fetchImpl: typeof fetch = async () => {
    calls += 1;
    return new Response('', { status: 401 });
  };
  const response = await handleSendGridWebhook(
    post(ENROLLED_KEY, {
      [SIGNATURE_HEADER]: signPayload(TIMESTAMP, BODY),
      [TIMESTAMP_HEADER]: TIMESTAMP,
    }),
    enrolledEnv,
    fetchImpl,
  );
  assert.equal(calls, 1);
  assert.equal(response.status, 401);
  assert.equal(await response.text(), 'Unauthorized');
});

test('accepts a signed request for an enrolled key the events API accepts', async () => {
  const calls: Array<{ url: string; apiKey: string | null }> = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    calls.push({
      url: String(input),
      apiKey: new Headers(init?.headers).get('statsig-api-key'),
    });
    return new Response('', { status: 202 });
  };
  const response = await handleSendGridWebhook(
    post(ENROLLED_KEY, {
      [SIGNATURE_HEADER]: signPayload(TIMESTAMP, BODY),
      [TIMESTAMP_HEADER]: TIMESTAMP,
    }),
    enrolledEnv,
    fetchImpl,
  );
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { success: true });
  assert.deepEqual(calls, [
    {
      url: 'https://events.statsigapi.net/v1/log_event',
      apiKey: ENROLLED_KEY,
    },
  ]);
});
