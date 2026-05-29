SET search_path TO vanta_core;

INSERT INTO workshops (
  id, name, slug, phone_number, whatsapp_phone_number_id, meta_business_account_id, subscription_plan
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  'Demo Workshop',
  'demo-workshop',
  '+27000000000',
  '100000000000000',
  '200000000000000',
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

INSERT INTO whatsapp_numbers (
  workshop_id, phone_number, whatsapp_phone_number_id, meta_business_account_id, access_token, webhook_verify_token
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  '+27000000000',
  '100000000000000',
  '200000000000000',
  'replace_with_meta_access_token',
  'replace_with_verify_token'
) ON CONFLICT (whatsapp_phone_number_id) DO NOTHING;

INSERT INTO messaging_accounts (
  workshop_id, provider, channel, account_id, sender_id, access_token, webhook_verify_token
) VALUES (
  '11111111-1111-1111-1111-111111111111',
  'meta',
  'whatsapp',
  '200000000000000',
  '100000000000000',
  'replace_with_meta_access_token',
  'replace_with_verify_token'
) ON CONFLICT (workshop_id, provider, channel, sender_id) DO NOTHING;
