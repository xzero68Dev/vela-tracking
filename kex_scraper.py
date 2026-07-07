"""
KEX Express API client — ดึงสถานะพัสดุโดยตรงจาก KEX API
ไม่ต้องการ browser, ไม่มีค่าใช้จ่าย
"""
import asyncio
import base64
from datetime import datetime
import httpx

KEX_STATUS_MAP = {
    "POD":    ("delivered",         "จัดส่งสำเร็จ"),
    "045":    ("out_for_delivery",  "กำลังจัดส่ง"),
    "040":    ("out_for_delivery",  "กำลังจัดส่ง"),
    "103":    ("in_transit",        "ถึงคลังปลายทาง"),
    "110":    ("in_transit",        "ออกจากศูนย์กระจาย"),
    "109":    ("in_transit",        "ถึงศูนย์กระจาย"),
    "010":    ("in_transit",        "รับพัสดุแล้ว"),
    "DROPPN": ("accepted",          "รับฝากที่สาขา KEX"),
    "005":    ("accepted",          "รับฝากแล้ว"),
    "RTN":    ("returned",          "ส่งคืนต้นทาง"),
    "RETURN": ("returned",          "ส่งคืนต้นทาง"),
    "FAIL":   ("problem",           "จัดส่งไม่สำเร็จ"),
}

def make_track_param(barcode: str) -> str:
    """สร้าง ?track= parameter — Base64 encode ของ barcode"""
    # KEX ใช้ Base64 ของ barcode เป็น URL parameter
    return base64.b64encode(barcode.encode()).decode()


async def fetch_kex_tracking(barcode: str) -> dict:
    """ดึงสถานะพัสดุ KEX ผ่าน API โดยตรง ไม่ต้องใช้ browser"""
    track_param = make_track_param(barcode)
    url = f"https://th.kex-express.com/th/track/?track={track_param}"

    headers = {
        "Accept":        "application/json, text/plain, */*",
        "Content-Type":  "application/json",
        "kett-lang":     "th",
        "X-KE-ID":       "{xkeid}",
        "Referer":       url,
        "User-Agent":    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36",
        "sec-ch-ua":     '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile":   "?1",
        "sec-ch-ua-platform": '"Android"',
    }

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            res = await client.post(url, json={"tracking_no": barcode}, headers=headers)

        if res.status_code != 200:
            print(f"[KEX API] {barcode} → HTTP {res.status_code}")
            return {"barcode": barcode, "status": "unknown", "status_th": "ไม่พบข้อมูล", "events": []}

        data = res.json()

        # response เป็น list
        if isinstance(data, list):
            data = data[0] if data else {}

        ref = data.get("ref", {})
        shipment = ref.get("shipment", {})
        statuses = ref.get("shipment_status", [])

        if not statuses:
            return {"barcode": barcode, "status": "unknown", "status_th": "ไม่พบข้อมูล", "events": []}

        # status ล่าสุด = indx สูงสุด
        latest = max(statuses, key=lambda x: x.get("indx", 0))
        s_code = latest.get("s_code", "")
        s_desc = latest.get("s_desc", "")
        status, status_th = KEX_STATUS_MAP.get(s_code, ("in_transit", s_desc))

        # events เรียงจากเก่าไปใหม่ (indx น้อยไปมาก)
        events = sorted(statuses, key=lambda x: x.get("indx", 0))
        events_clean = [
            {
                "datetime":    e.get("s_datetime", ""),
                "description": e.get("s_desc", ""),
                "status_th":   e.get("s_desc", ""),
                "location":    e.get("loc", ""),
            }
            for e in events
        ]

        return {
            "barcode":         barcode,
            "status":          status,
            "status_th":       status_th,
            "latest_location": latest.get("loc", ""),
            "events":          events_clean,
        }

    except Exception as e:
        print(f"[KEX API] error {barcode}: {e}")
        return {"barcode": barcode, "status": "error", "status_th": "เชื่อมต่อไม่ได้", "events": []}


# ทดสอบ manual
if __name__ == "__main__":
    import sys, json
    code = sys.argv[1] if len(sys.argv) > 1 else "SXF112970001475"
    result = asyncio.run(fetch_kex_tracking(code))
    print(json.dumps(result, ensure_ascii=False, indent=2))
