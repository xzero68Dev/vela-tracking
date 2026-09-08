-- P3: Event layer กลาง — เก็บ conversion event ทุกชนิดไว้ที่เดียว แล้วค่อย fan out ไปแต่ละแพลตฟอร์ม
-- event_id เป็น text (ไม่ใช่ uuid) เพื่อรองรับ dedupe key แบบ deterministic เช่น "purchase-<order_id>"
create table if not exists events (
  event_id     text primary key,          -- client generate (uuid) หรือ server ("purchase-<order_id>") — ใช้ dedupe
  event_name   text not null,             -- page_view | view_item | add_to_cart | begin_checkout | purchase
  occurred_at  timestamptz not null default now(),
  order_id     text,
  value        numeric,
  currency     text default 'THB',
  items        jsonb,                      -- [{ sku, qty, price }]
  email_sha256 text,                       -- hash เท่านั้น ห้ามเก็บ plaintext
  phone_sha256 text,
  click_id     text,                       -- gclid / fbclid / ตัวที่แพลตฟอร์มใหม่ใช้
  source       text,                       -- web | line | shopee
  user_agent   text,
  ip_hash      text,
  -- สถานะการส่งต่อไปแต่ละปลายทาง (dispatcher จะมาอัปเดต) — เก็บเป็น jsonb ยืดหยุ่น
  dispatched   jsonb default '{}'::jsonb,  -- { "google_ads": {"status":"sent","at":...}, ... }
  created_at   timestamptz not null default now()
);

create index if not exists idx_events_name_time on events (event_name, occurred_at desc);
create index if not exists idx_events_order      on events (order_id);
create index if not exists idx_events_purchase_pending
  on events (occurred_at) where event_name = 'purchase';

-- ปิด anon เข้าตรง — เขียน/อ่านผ่าน backend (service key) เท่านั้น (มี PII hash อยู่)
alter table events enable row level security;
