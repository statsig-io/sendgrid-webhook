# Sendgrid Event Hook

Statsig helps run A/B Tests for email campaigns via Sendgrid.

The worker returns 200 only when the `apikey` query parameter is enrolled in the `SENDGRID_VERIFICATION_KEYS` secret, the SendGrid event-webhook signature verifies against that key's pinned public key, and the Statsig events API accepts the SDK key. The verification public key is not taken from the request. A missing, empty, unknown, or rejected key is 401. A missing or invalid signature is 401.

`SENDGRID_VERIFICATION_KEYS` is a JSON object of SDK key to PEM public key. Set it with `wrangler secret put SENDGRID_VERIFICATION_KEYS`. Requests for a key that is not in the map are rejected. Enable signed event webhooks in SendGrid and enroll that verification public key before deploying this check.