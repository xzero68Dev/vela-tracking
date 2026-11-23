-- เพิ่มคอลัมน์ item_skus เก็บ "SKU code ต่อรายการ" ของออเดอร์เว็บ
-- เช่น "HONEY x1, ORIGINAL x2" — ใช้ให้ content id ฝั่ง server (Conversions API)
-- ตรงกับ Pixel ฝั่งเว็บเป๊ะ (pixel ใช้ i.sku = HONEY/ORIGINAL อยู่แล้ว)
--
-- ปลอดภัย/เป็น additive: โค้ด main.py เขียน/อ่าน item_skus แบบ best-effort
-- (ถ้ายังไม่รัน migration นี้ ระบบจะ fallback ไป map ชื่อ→SKU จากตาราง products อัตโนมัติ
--  จึงรัน migration นี้ตอนไหนก็ได้ ไม่ต้องรีบก่อน deploy)

ALTER TABLE orders ADD COLUMN IF NOT EXISTS item_skus text;
