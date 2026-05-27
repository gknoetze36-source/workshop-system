# Twilio And Paystack Setup

This guide is for configuring production messaging and payments for the VANTA workshop backend on Railway.

## Required Railway Variables

Add these to the backend Railway service:

```txt
PUBLIC_BASE_URL=https://your-backend-domain.up.railway.app

TWILIO_ACCOUNT_SID=your_twilio_account_sid
TWILIO_AUTH_TOKEN=your_twilio_auth_token
TWILIO_SMS_FROM=+27xxxxxxxxx
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886

PAYSTACK_SECRET_KEY=sk_live_xxxxxxxxxxxxxxxxx
PAYSTACK_WEBHOOK_SECRET=your_paystack_webhook_secret
```

For local testing, use sandbox/test keys and test sender numbers.

## Twilio Setup

1. Create or open your Twilio account.
2. Go to `Console` -> `Account Info`.
3. Copy:
   - `Account SID`
   - `Auth Token`
4. In Railway, set:
   - `TWILIO_ACCOUNT_SID`
   - `TWILIO_AUTH_TOKEN`

## SMS Sender

1. Buy or verify a Twilio phone number that supports SMS.
2. Add it to Railway:

```txt
TWILIO_SMS_FROM=+27xxxxxxxxx
```

Use international format.

## WhatsApp Sender

For testing, Twilio usually starts with the sandbox number:

```txt
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
```

For production WhatsApp:

1. Complete Twilio WhatsApp Business approval.
2. Connect the approved WhatsApp sender.
3. Replace `TWILIO_WHATSAPP_FROM` with the approved sender:

```txt
TWILIO_WHATSAPP_FROM=whatsapp:+27xxxxxxxxx
```

## Twilio Webhook

The backend receives inbound WhatsApp messages here:

```txt
POST /webhook/twilio/<franchise_slug>/<branch_slug>/<token>
```

Full example:

```txt
https://your-backend-domain.up.railway.app/webhook/twilio/demo-motor-group/main-branch/secret-token
```

The token is the franchise `inbound_webhook_token`.

Set this URL in Twilio:

`Messaging` -> `WhatsApp Senders` or `Sandbox Settings` -> `When a message comes in`

Use:

```txt
Method: POST
```

## Paystack Setup

1. Create or open your Paystack account.
2. Go to `Settings` -> `API Keys & Webhooks`.
3. Copy your secret key.
4. Add it to Railway:

```txt
PAYSTACK_SECRET_KEY=sk_live_xxxxxxxxxxxxxxxxx
```

For test mode, use:

```txt
PAYSTACK_SECRET_KEY=sk_test_xxxxxxxxxxxxxxxxx
```

## Paystack Webhook

Set the Paystack webhook URL to:

```txt
https://your-backend-domain.up.railway.app/webhook/paystack
```

The backend listens for successful charges and marks matching billing periods as paid.

## Paystack Webhook Secret

Set:

```txt
PAYSTACK_WEBHOOK_SECRET=your_paystack_webhook_secret
```

If this is not set, the backend falls back to `PAYSTACK_SECRET_KEY` for signature verification. A separate webhook secret is cleaner for production.

## Payment Links

Payment links are created from the admin billing screen.

Backend route:

```txt
POST /billing/<billing_id>/payment-link
```

The backend uses:

```txt
PUBLIC_BASE_URL
PAYSTACK_SECRET_KEY
```

to create the Paystack transaction and save the payment link.

## Final Checklist

- `PUBLIC_BASE_URL` is the real backend Railway URL.
- Twilio SID and auth token are saved in Railway.
- SMS sender is in international format.
- WhatsApp sender starts with `whatsapp:`.
- Twilio inbound webhook points to `/webhook/twilio/<franchise_slug>/<branch_slug>/<token>`.
- Paystack secret key is live for production.
- Paystack webhook points to `/webhook/paystack`.
- A franchise has a valid `inbound_webhook_token`.
- Restart or redeploy the Railway service after changing variables.

