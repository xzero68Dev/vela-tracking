-- ============================================================
-- VeLA Referral System — migration (v1.0, 2026-09-21)
-- ปลอดภัย 100%: คอลัมน์ใหม่ nullable + default, ตารางใหม่แยก
-- ระบบขายเดิมไม่กระทบ (insert order/customer เดิมทำงานปกติ)
-- รันใน Supabase SQL Editor ได้เลย รันซ้ำได้ (idempotent)
-- ============================================================

-- ---- customers: ธงผู้แนะนำ + โค้ด + พร้อมเพย์ ----
ALTER TABLE customers ADD COLUMN IF NOT EXISTS is_referrer boolean DEFAULT false;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS ref_code    text;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS promptpay   text;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS ref_clicks  integer DEFAULT 0;
-- unique เฉพาะค่าที่ไม่ null (กันโค้ดซ้ำ แต่คนที่ยังไม่สมัครเป็น null ได้หลายคน)
CREATE UNIQUE INDEX IF NOT EXISTS customers_ref_code_uniq ON customers (ref_code) WHERE ref_code IS NOT NULL;

-- ---- orders: โค้ดที่ติดมากับออเดอร์ (ถ้ามี) ----
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ref_code text;

-- ---- ตารางผูกลูกค้า ↔ ผู้แนะนำ (12 เดือน) ----
CREATE TABLE IF NOT EXISTS referral_bindings (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  referrer_phone text NOT NULL,               -- เบอร์ผู้แนะนำ (normalize)
  customer_phone text NOT NULL UNIQUE,        -- ลูกค้า 1 คนผูกได้ผู้แนะนำเดียว
  bound_at       timestamptz NOT NULL,        -- วันที่ออเดอร์แรก paid
  expires_at     timestamptz NOT NULL         -- bound_at + 12 เดือน
);
CREATE INDEX IF NOT EXISTS referral_bindings_referrer_idx ON referral_bindings (referrer_phone);

-- ---- ตารางรายการคอม ----
CREATE TABLE IF NOT EXISTS referral_earnings (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  referrer_phone text NOT NULL,
  customer_phone text NOT NULL,
  order_id       text NOT NULL,               -- 1 ออเดอร์ = 1 แถว commission (kind อื่นได้อีก)
  base_amount    numeric NOT NULL DEFAULT 0,  -- ยอดหลังส่วนลด ไม่รวมส่ง
  commission     numeric NOT NULL DEFAULT 0,  -- 10% หรือ +โบนัส 30 หรือติดลบ (reversal)
  kind           text NOT NULL,               -- 'commission' | 'bonus_first5' | 'reversal'
  status         text NOT NULL DEFAULT 'pending',  -- pending → confirmed → paid
  created_at     timestamptz DEFAULT now(),
  paid_at        timestamptz
);
-- กันคอมซ้ำ: 1 order มี commission ได้ 1, bonus ได้ 1, reversal ได้ 1 (คนละ kind)
CREATE UNIQUE INDEX IF NOT EXISTS referral_earnings_order_kind_uniq ON referral_earnings (order_id, kind);
CREATE INDEX IF NOT EXISTS referral_earnings_referrer_idx ON referral_earnings (referrer_phone, status);

-- ---- RLS: เปิดไว้ ไม่มี policy anon (backend ใช้ service key เท่านั้น) ----
ALTER TABLE referral_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE referral_earnings ENABLE ROW LEVEL SECURITY;
