-- ส่วนลดลูกค้า VIP: % ต่อคน (เพดานคิดที่ backend = ฿130)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS vip_discount_pct INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN customers.vip_discount_pct IS 'ส่วนลด VIP เป็น % (0-100), 0 = ไม่ใช่ VIP. คิดเลือกอันที่ลดมากกว่าเทียบกับโปรลูกค้าใหม่ เพดาน ฿130';
