-- เพิ่มคอลัมน์ขนส่งที่ลูกค้าเลือกตอน checkout
-- รันใน Supabase SQL Editor "ก่อน" deploy backend/หน้าเว็บใหม่
-- ค่าที่เก็บ: 'thailand_post' (ไปรษณีย์ไทย EMS) หรือ 'kex'
ALTER TABLE orders ADD COLUMN IF NOT EXISTS preferred_carrier text;
