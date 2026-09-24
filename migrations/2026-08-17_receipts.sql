-- ตาราง receipts: เก็บใบเสร็จรับเงินเพื่อสร้าง "ลิงก์ถาวรให้ลูกค้า"
-- รันใน Supabase SQL Editor "ก่อน" deploy backend เวอร์ชันใหม่ (รันซ้ำได้ปลอดภัย)
--
-- token   = ส่วนลับในลิงก์ https://velacoldbrew.com/receipt/<token> (เดาไม่ได้)
-- order_id = อ้างอิงออเดอร์ (unique → ออกใบเสร็จซ้ำ = อัปเดตใบเดิม ลิงก์ไม่เปลี่ยน)
-- data    = jsonb เก็บข้อมูลใบเสร็จทั้งใบ (รายการ/ยอด/ลูกค้า) ใช้ regenerate PDF ได้ตลอด
-- หมายเหตุ: ไม่มี FK ไป orders เพราะต้องรองรับ Shopee ที่บางทีไม่มี row ใน orders

CREATE TABLE IF NOT EXISTS receipts (
    token      text PRIMARY KEY,
    order_id   text,
    channel    text,
    data       jsonb NOT NULL,
    created_at timestamptz DEFAULT now()
);

-- ออเดอร์หนึ่งมีใบเสร็จ (ลิงก์) เดียว — upsert by order_id ในโค้ดอาศัย unique นี้
CREATE UNIQUE INDEX IF NOT EXISTS receipts_order_id_key ON receipts (order_id);

-- ===== ความปลอดภัย: เปิด RLS แต่ "ไม่ใส่ policy ให้ anon" =====
-- data มี PII ลูกค้า (ชื่อ/ที่อยู่/เบอร์) — anon key โผล่ในหน้าเว็บ ถ้าไม่เปิด RLS
-- ใครก็ยิง REST ด้วย anon key แล้วดึงใบเสร็จทั้งหมดได้
-- เปิด RLS + ไม่มี policy → anon/public โดนปฏิเสธทั้งหมด
-- backend ใช้ service key (บายพาส RLS) จึงยังอ่าน/เขียนได้ปกติ → ทุก endpoint ทำงานเหมือนเดิม
ALTER TABLE receipts ENABLE ROW LEVEL SECURITY;
