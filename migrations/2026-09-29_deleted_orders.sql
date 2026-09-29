-- Archive ออเดอร์ที่ถูกลบ (หมดเวลา/ลูกค้าลบเอง) แทนการลบทิ้งถาวร
-- เก็บทั้งแถวเป็น jsonb → กู้คืนได้แม้ schema orders เปลี่ยนภายหลัง · รันซ้ำได้
create table if not exists deleted_orders (
  id          bigserial primary key,
  order_id    text,
  phone       text,
  customer    text,
  reason      text,                              -- expired_unpaid | customer_deleted
  deleted_at  timestamptz not null default now(),
  data        jsonb not null                     -- ทั้งแถว orders เดิม
);

create index if not exists idx_deleted_orders_phone    on deleted_orders (phone);
create index if not exists idx_deleted_orders_order_id on deleted_orders (order_id);
create index if not exists idx_deleted_orders_at       on deleted_orders (deleted_at desc);

-- RLS เปิด ไม่มี policy = anon เข้าไม่ได้ (backend ใช้ service key ข้าม RLS) — เหมือนตารางอื่น
alter table deleted_orders enable row level security;
