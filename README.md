# VeLA Tracking API

Backend สำหรับเช็คสถานะพัสดุไปรษณีไทย — สร้างสำหรับ VeLA Cold Brew

## Endpoints

| Method | Path | คำอธิบาย |
|--------|------|-----------|
| GET | `/health` | ตรวจสอบว่า service ทำงานอยู่ |
| GET | `/track/{barcode}` | เช็คสถานะพัสดุ 1 ชิ้น |
| POST | `/track/bulk` | เช็คพร้อมกันสูงสุด 20 เลข |

### ตัวอย่าง bulk request
```json
POST /track/bulk
{
  "barcodes": ["JM123456789TH", "JM987654321TH"]
}
```

### Status values
| status | ความหมาย |
|--------|-----------|
| `accepted` | รับฝากแล้ว |
| `in_transit` | อยู่ระหว่างขนส่ง |
| `out_for_delivery` | ออกนำจ่ายแล้ว |
| `delivered` | จัดส่งสำเร็จ ✓ |
| `returned` | ตีกลับ ⚠ |
| `problem` | มีปัญหา ⚠ |
| `unknown` | ไม่ทราบสถานะ |

---

## รันบนเครื่อง (Local)

```bash
# 1. ติดตั้ง dependencies
pip install -r requirements.txt

# 2. สร้าง .env
cp .env.example .env
# แก้ไข .env ใส่ THAIPOST_API_KEY ของคุณ

# 3. รัน
uvicorn main:app --reload

# เปิด http://localhost:8000/docs เพื่อทดสอบ API
```

---

## Deploy บน Railway

1. Push โค้ดขึ้น GitHub
2. ไปที่ [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. เลือก repo นี้
4. ไปที่ Variables → เพิ่ม `THAIPOST_API_KEY`
5. Railway จะ deploy อัตโนมัติ ได้ URL ทันที

## Deploy บน Render

1. Push โค้ดขึ้น GitHub
2. ไปที่ [render.com](https://render.com) → New Web Service → Connect GitHub
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Environment Variables → เพิ่ม `THAIPOST_API_KEY`
