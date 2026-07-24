-- แก้บั๊ก: ลูกค้า login ด้วยเบอร์/OTP (และ guest ที่เก็บอัตโนมัติตอนสั่งซื้อ) ไม่มี LINE ID
-- แต่ customers.line_user_id ตั้ง NOT NULL → insert ล้ม (23502) → /auth/verify-otp คืน 500
-- แก้ให้ line_user_id เป็น NULL ได้ (ลูกค้าที่ผูกกับ LINE ค่อยมีค่า)

ALTER TABLE customers ALTER COLUMN line_user_id DROP NOT NULL;

-- หมายเหตุ: ถ้ามี UNIQUE บน line_user_id อยู่แล้ว ไม่ต้องแก้ —
-- Postgres ถือว่า NULL แต่ละแถวไม่ซ้ำกัน หลายแถวเป็น NULL ได้ปกติ
