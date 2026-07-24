-- DooSlip — ตารางสำหรับบอตอ่านสลิปในกลุ่ม LINE (รวมเข้า Supabase เดียวกับ vela)
-- รันใน Supabase SQL Editor ของโปรเจกต์ vela

-- ร้าน/กลุ่ม LINE (whitelist + ตั้งชื่อร้าน/บัญชี)
create table if not exists shops (
  id           uuid default gen_random_uuid() primary key,
  group_id     text unique not null,
  shop_name    text default '',
  account_name text,
  approved     boolean default false,
  created_at   timestamptz default now()
);
create index if not exists idx_shops_group on shops(group_id);

-- สลิปที่บันทึกจากกลุ่ม
create table if not exists slips (
  id            uuid default gen_random_uuid() primary key,
  group_id      text not null,
  user_id       text,
  display_name  text,
  amount        numeric,
  transfer_date date,
  transfer_time text,
  ref_number    text,
  sender_name   text,
  receiver_name text,
  bank          text,
  created_at    timestamptz default now()
);
create index if not exists idx_slips_ref_group   on slips(ref_number, group_id);
create index if not exists idx_slips_group_date  on slips(group_id, created_at desc);
