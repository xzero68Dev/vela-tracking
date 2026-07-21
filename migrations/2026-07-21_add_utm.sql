-- เพิ่มคอลัมน์ UTM tracking ให้ตาราง orders
-- รันใน Supabase SQL Editor "ก่อน" deploy backend เวอร์ชันใหม่
-- (IF NOT EXISTS รันซ้ำได้ปลอดภัย ไม่พังถ้ามีคอลัมน์อยู่แล้ว)

ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_source   text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_medium   text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_campaign text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_content  text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_term     text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS referrer     text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS landing_page text;

-- (ตัวเลือก) index ช่วยให้ group by แคมเปญเร็วขึ้นเวลาทำรายงาน
CREATE INDEX IF NOT EXISTS idx_orders_utm_campaign ON orders (utm_campaign);
CREATE INDEX IF NOT EXISTS idx_orders_utm_source   ON orders (utm_source);
