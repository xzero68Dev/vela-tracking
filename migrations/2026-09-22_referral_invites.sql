-- ============================================================
-- VeLA Referral — invite codes + waitlist (2026-09-22)
-- เฟส invite-only: นักรีวิวได้ลิงก์ velacoldbrew.com/referral?invite=XXXX
-- โค้ดใช้ครั้งเดียวต่อคน · waitlist เก็บคนสนใจระหว่างทดสอบ
-- ปลอดภัย 100%: ตารางใหม่แยก ไม่กระทบของเดิม · รันซ้ำได้
-- ============================================================

-- ---- โค้ดเชิญ ----
CREATE TABLE IF NOT EXISTS referral_invites (
  code       text PRIMARY KEY,            -- โค้ดเชิญ (a-z0-9) — สร้างจากหลังบ้าน
  note       text,                        -- โน้ต เช่น ชื่อนักรีวิว/กลุ่ม
  created_at timestamptz DEFAULT now(),
  used_by    text,                        -- เบอร์ผู้ที่ใช้โค้ดนี้ (null = ยังไม่ถูกใช้)
  used_at    timestamptz
);

-- ---- waitlist คนสนใจร่วมโปรแกรม ----
CREATE TABLE IF NOT EXISTS referral_waitlist (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name         text,
  contact      text,                      -- เบอร์/LINE/ช่องทางติดต่อ
  line_user_id text,
  note         text,
  created_at   timestamptz DEFAULT now()
);

-- RLS: เปิดไว้ ไม่มี policy anon (backend ใช้ service key เท่านั้น)
ALTER TABLE referral_invites  ENABLE ROW LEVEL SECURITY;
ALTER TABLE referral_waitlist ENABLE ROW LEVEL SECURITY;

-- ---- ตัวอย่างสร้างโค้ดเชิญ (แก้ code/note ตามต้องการ) ----
-- INSERT INTO referral_invites (code, note) VALUES
--   ('vela01', 'นักรีวิว กลุ่มแม่บ้าน A'),
--   ('vela02', 'พี่รีวิว IG');
