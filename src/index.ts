/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `wrangler dev src/index.ts` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `wrangler publish src/index.ts --name my-worker` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

export default {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('Invalid request', {
        status: 405,
      });
    }

    const apiKey = this.getApiKey(request);
    if (!apiKey) {
      return new Response('Unauthorized', {
        status: 401,
      });
    }

    try {
      const payload = await request.json();
      return await this.processWebhook(apiKey, payload);
    } catch {
      return new Response('Invalid request', {
        status: 400,
      });
    }
  },

  convertEventToExposure(
    evt: Record<string, unknown>,
  ): Record<string, unknown> | null {
    if (!evt.user || !evt.metadata || typeof evt.metadata !== 'object') {
      return null;
    }
    const experimentPath = (evt.metadata as any).singlesend_name;
    if (!experimentPath || typeof experimentPath !== 'string') {
      return null;
    }
    const paths = experimentPath.split('/');
    if (paths.length !== 2) {
      return null;
    }
    const variant = paths[1].toLowerCase() === 'test' ? 'Test' : 'Control';
    
    return {
      user: evt.user,
      experimentName: paths[0],
      group: variant,
    };
  },

  convertSingleEvent(item: any): Record<string, unknown> | null {
    if (typeof item !== 'object' || !item.event) {
      return null;
    }
    const evt = {} as Record<string, unknown>;
    evt.eventName = item.event;
    evt.time = item.timestamp;
    evt.user = {
      userID: this.getHash(item.email),
      email: item.email,
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

  getApiKey(request: Request): string | null {
    const url = new URL(request.url);
    return url.searchParams.get('apikey');    
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

  async logEvents(apiKey: string, events: Array<Record<string, unknown>>) {
    await this.postToScrapi('log_event', apiKey, { events });
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
  ) {
    try {
      const resp = await fetch(
        `https://events.statsigapi.net/v1/${endpoint}`, 
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
        console.log('Scrapi POST failed');
      }
    } catch (err: any) {
      console.log(err);
    }
  },

  async processExposures(
    apiKey: string,
    events: Array<Record<string, unknown>>
  ) {
    const expEvents = events.filter(evt => evt.eventName === 'delivered');
    if (expEvents.length === 0) {
      return;
    }
    const exposures = [];
    for (let ii = 0; ii < expEvents.length; ii++) {
      const exp = this.convertEventToExposure(expEvents[ii]);
      if (exp) {
        exposures.push(exp);
      }
    }
    await this.postToScrapi(
      'log_custom_exposure',
      apiKey,
      { exposures },
    );
  },

  async processWebhook(apiKey: string, payload: unknown): Promise<Response> {
    const events = this.convertToStatsigEvents(payload);
    if (events.length === 0) {
      return new Response('Unexpected content format', { status: 406 });  
    }

    await this.logEvents(apiKey, events);
    await this.processExposures(apiKey, events);
    return new Response(JSON.stringify({ success: true }), {
      status: 200,
    });
  },
};
