-- OpenAI/ChatGPT Ads conversion tracking — เก็บ click id (oppref/obref) + utm + เวลาที่ยิง Conversions API
-- รันซ้ำได้ (if not exists) · additive ไม่กระทบข้อมูลเดิม
alter table orders
  add column if not exists oppref       text,
  add column if not exists obref        text,
  add column if not exists utm_source   text,
  add column if not exists utm_medium   text,
  add column if not exists utm_campaign text,
  add column if not exists utm_content  text,
  add column if not exists oai_sent_at  timestamptz;
