-- รูปหลักฐานการจัดส่ง (กรณีส่งเอง) — เก็บ URL รูปที่แอดมินอัปตอนกดยืนยันส่งถึง
-- additive nullable ไม่กระทบของเดิม · รันซ้ำได้
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_photo text;
