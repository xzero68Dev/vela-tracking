-- เพิ่มคอลัมน์เก็บ "ยอดส่วนลดลูกค้าใหม่จริง" (บาท) ให้ตาราง orders
-- รันใน Supabase SQL Editor "ก่อน" เปิดโปร 50% (ไม่งั้น order ที่มีส่วนลดจะ insert ไม่ผ่าน)
-- order ปกติ (ไม่มีส่วนลด) ไม่กระทบ เพราะ backend ใส่คอลัมน์นี้เฉพาะเมื่อมีส่วนลด

ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_amount numeric DEFAULT 0;
