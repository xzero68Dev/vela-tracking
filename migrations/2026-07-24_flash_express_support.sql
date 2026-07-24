-- Flash Express (Shopee-managed) support
-- ผ่อน constraint ให้รับออเดอร์ Flash ได้: phone "-", carrier "Flash Express", shipping_cost 0, tracking THxxxxC
--
-- หมายเหตุ: ตาราง orders/shipping ปัจจุบันเก็บ carrier เป็น text อิสระ และ import ก็ใส่ค่าได้หลายแบบอยู่แล้ว
-- migration นี้ส่วนใหญ่จะเป็น no-op — รันเพื่อความชัวร์ / รันถ้าเจอ error ตอน insert ออเดอร์ Flash

-- 1) เบอร์ลูกค้าถูก Shopee ซ่อน → orders.phone ต้องว่าง/"-" ได้
ALTER TABLE orders ALTER COLUMN phone DROP NOT NULL;

-- 2) ถ้ามี CHECK constraint เดิมบังคับรูปแบบ carrier / phone / shipping_cost
--    ให้ลบทิ้ง (แก้ชื่อ constraint ตามจริงใน Supabase → Database → Constraints)
--    ตัวอย่าง (uncomment + แก้ชื่อถ้ามีจริง):
-- ALTER TABLE shipping DROP CONSTRAINT IF EXISTS shipping_carrier_check;
-- ALTER TABLE orders   DROP CONSTRAINT IF EXISTS orders_phone_check;
-- ALTER TABLE shipping DROP CONSTRAINT IF EXISTS shipping_shipping_cost_check;

-- carrier ที่รองรับ: 'Flash Express' | 'POST SABUY' | 'KEX Express' | 'Seller Own Fleet'
-- tracking prefix:   Flash = TH…C | KEX = SXF… | POST SABUY = JM…TH | ส่งเอง = '-'
