/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `wrangler dev src/index.ts` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `wrangler publish src/index.ts --name my-worker` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import {
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  parseVerificationKeys,
  readApiKey,
  verifySendGridSignature,
} from './auth.ts';

export interface Env {
  SENDGRID_VERIFICATION_KEYS?: string;
}

const EVENTS_API = 'https://events.statsigapi.net/v1';

function unauthorized(): Response {
  return new Response('Unauthorized', { status: 401 });
}

function responseForUpstreamStatus(status: number): Response | null {
  if (status >= 200 && status < 300) {
    return null;
  }
  if (status === 401 || status === 403) {
    return unauthorized();
  }
  return new Response('Upstream request failed', { status: 502 });
}

export async function handleSendGridWebhook(
  request: Request,
  env: Env = {},
  fetchImpl: typeof fetch = fetch,
): Promise<Response> {
  if (request.method !== 'POST') {
    return new Response('Invalid request', { status: 405 });
  }

  const apiKey = readApiKey(request.url);
  if (!apiKey) {
    return unauthorized();
  }

  const verificationKey = parseVerificationKeys(env.SENDGRID_VERIFICATION_KEYS)[apiKey];
  if (!verificationKey) {
    return unauthorized();
  }

  const rawBody = await request.text();
  const signatureOk = await verifySendGridSignature(
    verificationKey,
    rawBody,
    request.headers.get(SIGNATURE_HEADER),
    request.headers.get(TIMESTAMP_HEADER),
  );
  if (!signatureOk) {
    return unauthorized();
  }

  let payload: unknown;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return new Response('Invalid request', { status: 400 });
  }

  return worker.processWebhook(apiKey, payload, fetchImpl);
}

const worker = {

  convertEventToExposure(
    evt: Record<string, unknown>,
  ): Record<string, unknown> | null {
    if (!evt.user) {
      return null;
    }
    const expDetails = this.getExperimentDetails(evt);
    if (!expDetails || expDetails.length !== 2) {
      return null;
    }
    return {
      user: evt.user,
      experimentName: expDetails[0],
      group: expDetails[1],
    };
  },

  convertSingleEvent(item: any): Record<string, unknown> | null {
    if (typeof item !== 'object' || !item.event) {
      return null;
    }
    const evt = {} as Record<string, unknown>;
    evt.eventName = item.event;
    evt.time = item.timestamp;
    const sid = this.getHash(item.email);
    evt.user = {
      userID: sid,
      email: item.email,
      user: {
        customIDs: {
          stableID: sid,
        }
      },
    };
    evt.metadata = this.normalizeMetadata(item);
    return evt;
  },
  
  convertToStatsigEvents(payload: unknown): Array<Record<string, unknown>> {
    const events = [];
    if (Array.isArray(payload)) {
      for (let ii = 0; ii < payload.length; ii++) {
        const evt = this.convertSingleEvent(payload[ii]);
        if (evt) {
          events.push(evt);
        }
      }
    } else {
      const evt = this.convertSingleEvent(payload);
      if (evt) {
        events.push(evt);
      }
    }
    return events;
  },

  async fetch(request: Request, env?: Env): Promise<Response> {
    return handleSendGridWebhook(request, env ?? {});
  },

  getExperimentDetails(evt: Record<string, unknown>): Array<string> | null {
    if (!evt.user || !evt.metadata || typeof evt.metadata !== 'object') {
      return null;
    }
    const metadata = evt.metadata as any;
    const experimentName = metadata.statsig_experiment_name;
    const variantName = metadata.statsig_variant_name;
    if (experimentName && variantName) {
      return [experimentName, variantName];
    }

    // Fallback is a path stored in single send name
    const experimentPath = metadata.singlesend_name;
    if (!experimentPath || typeof experimentPath !== 'string') {
      return null;
    }
    const paths = experimentPath.split('/');
    if (paths.length !== 2) {
      return null;
    }
    let variant = paths[1].toLowerCase();
    variant = (variant === 'test' ? 'Test' : variant);
    variant = (variant === 'control' ? 'Control' : variant);
    return [paths[0], variant];
  },

  getHash(text: any): string {
    if (typeof text !== 'string') {
      return 'unknown_id';
    }
    const seed = 0x12475790;
    let h1 = 0xdeadbeef ^ seed,
    h2 = 0x41c6ce57 ^ seed;
    for (let i = 0, ch; i < text.length; i++) {
      ch = text.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);    
    return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16);
  },

  async logEvents(
    apiKey: string,
    events: Array<Record<string, unknown>>,
    fetchImpl: typeof fetch,
  ): Promise<number> {
    return this.postToScrapi('log_event', apiKey, { events }, fetchImpl);
  },

  normalizeMetadata(item: any): Record<string, string> {
    const skippedFields = ['email', 'timestamp', 'event'];
    const metadata = {} as Record<string, string>;
    for (let key in item) {
      if (skippedFields.includes(key)) {
        continue;
      }
      const value = item[key];
      metadata[key] = 
        (typeof value === 'string') ? value : JSON.stringify(value);
    }
    return metadata;
  },

  async postToScrapi(
    endpoint: string,
    apiKey: string,
    body: Record<string, unknown>,
    fetchImpl: typeof fetch,
  ): Promise<number> {
    try {
      const resp = await fetchImpl(
        `${EVENTS_API}/${endpoint}`,
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'statsig-api-key': apiKey,
          },
          body: JSON.stringify(body),
        },
      );
      if (!resp.ok) {
        console.log(`Scrapi POST failed status=${resp.status}`);
      }
      return resp.status;
    } catch {
      console.log('Scrapi POST failed');
      return 502;
    }
  },

  async processExposures(
    apiKey: string,
    events: Array<Record<string, unknown>>,
    fetchImpl: typeof fetch,
  ): Promise<number | null> {
    const expEvents = events.filter(evt => evt.eventName === 'delivered');
    if (expEvents.length === 0) {
      return null;
    }
    const exposures = [];
    for (let ii = 0; ii < expEvents.length; ii++) {
      const exp = this.convertEventToExposure(expEvents[ii]);
      if (exp) {
        exposures.push(exp);
      }
    }
    if (exposures.length === 0) {
      return null;
    }
    return this.postToScrapi(
      'log_custom_exposure',
      apiKey,
      { exposures },
      fetchImpl,
    );
  },

  async processWebhook(
    apiKey: string,
    payload: unknown,
    fetchImpl: typeof fetch,
  ): Promise<Response> {
    const events = this.convertToStatsigEvents(payload);
    if (events.length === 0) {
      return new Response('Unexpected content format', { status: 406 });
    }

    const logStatus = await this.logEvents(apiKey, events, fetchImpl);
    const logFailure = responseForUpstreamStatus(logStatus);
    if (logFailure) {
      return logFailure;
    }

    const exposureStatus = await this.processExposures(apiKey, events, fetchImpl);
    if (exposureStatus != null) {
      const exposureFailure = responseForUpstreamStatus(exposureStatus);
      if (exposureFailure) {
        return exposureFailure;
      }
    }

    return new Response(JSON.stringify({ success: true }), {
      status: 200,
    });
  },
};

export default worker;
