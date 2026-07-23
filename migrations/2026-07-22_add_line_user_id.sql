-- เก็บ line_user_id ของ "คนที่ login สั่ง" ลงออเดอร์ เพื่อผูกแต้มเข้าบัญชีคนสั่ง
-- (แต้มจะไม่กระจายตามเบอร์ผู้รับเวลาส่งหลายที่อยู่/ส่งให้คนอื่น)
-- รันใน Supabase SQL Editor
ALTER TABLE orders ADD COLUMN IF NOT EXISTS line_user_id text;
