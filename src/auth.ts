export const SIGNATURE_HEADER = 'X-Twilio-Email-Event-Webhook-Signature';
export const TIMESTAMP_HEADER = 'X-Twilio-Email-Event-Webhook-Timestamp';

const P256_COORD_BYTES = 32;

export function readApiKey(requestUrl: string): string | null {
  let value: string | null;
  try {
    value = new URL(requestUrl).searchParams.get('apikey');
  } catch {
    return null;
  }
  if (value == null) {
    return null;
  }
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }
  return trimmed;
}

export function parseVerificationKeys(
  raw: string | undefined,
): Record<string, string> {
  if (raw == null || raw.trim() === '') {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (parsed == null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {};
    }
    const keys: Record<string, string> = {};
    for (const [apiKey, publicKey] of Object.entries(parsed)) {
      if (typeof publicKey !== 'string') {
        continue;
      }
      const id = apiKey.trim();
      const pem = publicKey.trim();
      if (id.length === 0 || pem.length === 0) {
        continue;
      }
      keys[id] = pem;
    }
    return keys;
  } catch {
    return {};
  }
}

export async function verifySendGridSignature(
  publicKeyPem: string,
  rawBody: string,
  signature: string | null,
  timestamp: string | null,
): Promise<boolean> {
  if (!signature || !timestamp || !/^[0-9]+$/.test(timestamp)) {
    return false;
  }
  try {
    const spki = pemToSpkiBytes(publicKeyPem);
    const signatureBytes = base64ToBytes(signature);
    if (!spki || !signatureBytes) {
      return false;
    }
    // SendGrid's ECDSA signature is DER. WebCrypto verifies raw P-256 r||s.
    const rawSignature = derSignatureToRaw(signatureBytes, P256_COORD_BYTES);
    if (!rawSignature) {
      return false;
    }
    const key = await crypto.subtle.importKey(
      'spki',
      spki,
      { name: 'ECDSA', namedCurve: 'P-256' },
      false,
      ['verify'],
    );
    return await crypto.subtle.verify(
      { name: 'ECDSA', hash: 'SHA-256' },
      key,
      rawSignature,
      new TextEncoder().encode(timestamp + rawBody),
    );
  } catch {
    return false;
  }
}

function pemToSpkiBytes(pem: string): Uint8Array | null {
  const b64 = pem
    .replace(/-----BEGIN PUBLIC KEY-----/g, '')
    .replace(/-----END PUBLIC KEY-----/g, '')
    .replace(/\s+/g, '');
  if (b64.length === 0) {
    return null;
  }
  return base64ToBytes(b64);
}

function base64ToBytes(value: string): Uint8Array | null {
  try {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  } catch {
    return null;
  }
}

function derSignatureToRaw(
  der: Uint8Array,
  size: number,
): Uint8Array | null {
  let offset = 0;
  if (der[offset++] !== 0x30) {
    return null;
  }
  const seqLen = readDerLength(der, offset);
  if (!seqLen) {
    return null;
  }
  offset = seqLen.offset;
  if (offset + seqLen.length !== der.length) {
    return null;
  }
  const r = readDerInteger(der, offset, size);
  if (!r) {
    return null;
  }
  offset = r.offset;
  const s = readDerInteger(der, offset, size);
  if (!s || s.offset !== der.length) {
    return null;
  }
  const raw = new Uint8Array(size * 2);
  raw.set(r.bytes, 0);
  raw.set(s.bytes, size);
  return raw;
}

function readDerLength(
  der: Uint8Array,
  offset: number,
): { length: number; offset: number } | null {
  if (offset >= der.length) {
    return null;
  }
  let length = der[offset++];
  if (length & 0x80) {
    const count = length & 0x7f;
    if (count === 0 || count > 2 || offset + count > der.length) {
      return null;
    }
    length = 0;
    for (let i = 0; i < count; i++) {
      length = (length << 8) | der[offset++];
    }
  }
  if (offset + length > der.length) {
    return null;
  }
  return { length, offset };
}

function readDerInteger(
  der: Uint8Array,
  offset: number,
  size: number,
): { bytes: Uint8Array; offset: number } | null {
  if (der[offset++] !== 0x02) {
    return null;
  }
  const lenInfo = readDerLength(der, offset);
  if (!lenInfo || lenInfo.length === 0) {
    return null;
  }
  offset = lenInfo.offset;
  const integer = der.subarray(offset, offset + lenInfo.length);
  offset += lenInfo.length;
  let start = 0;
  while (start < integer.length - 1 && integer[start] === 0) {
    start++;
  }
  const trimmed = integer.subarray(start);
  if (trimmed.length > size) {
    return null;
  }
  const bytes = new Uint8Array(size);
  bytes.set(trimmed, size - trimmed.length);
  return { bytes, offset };
}
