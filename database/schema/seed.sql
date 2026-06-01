SET search_path TO vanta_core;

INSERT INTO workshops (
  id, name, slug, phone_number, subscription_plan
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  'Demo Workshop',
  'demo-workshop',
  '+27000000000',
  'starter'
) ON CONFLICT (slug) DO NOTHING;

INSERT INTO users (
  workshop_id, full_name, email, password_hash, role
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  'Demo Owner',
  'owner@demo-workshop.test',
  '$2b$12$replace_with_real_hash',
  'owner'
) ON CONFLICT (workshop_id, email) DO NOTHING;

INSERT INTO messaging_accounts (
  workshop_id, provider, channel, sender_id, phone_number_id, access_token, webhook_verify_token, webhook_secret, embedded_signup_state, coexistence_status
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  'meta',
  'whatsapp',
  '+27000000000',
  'replace_with_meta_phone_number_id',
  'replace_with_meta_access_token',
  'replace_with_meta_verify_token',
  'replace_with_meta_webhook_secret',
  'not_started',
  'not_started'
) ON CONFLICT (workshop_id, provider, channel, sender_id) DO NOTHING;
