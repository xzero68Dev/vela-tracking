-- แจ้งเตือนออเดอร์ค้างชำระแบบบันได: เก็บระดับที่ส่งไปแล้ว (0=ยังไม่ส่ง, 1=~3ชม, 2=~24ชม, 3=~72ชม)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS reminder_stage INTEGER NOT NULL DEFAULT 0;
