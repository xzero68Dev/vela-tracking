-- เก็บเบอร์บัญชีของ "คนที่ login สั่ง" (LINE/OTP) เพื่อผูกแต้มเข้าบัญชีคนสั่ง
-- ครอบคลุมลูกค้าที่สมัคร/login ด้วยเบอร์โทร (OTP) ที่ไม่มี line_user_id
ALTER TABLE orders ADD COLUMN IF NOT EXISTS account_phone text;
