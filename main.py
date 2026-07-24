import os
import re
import time
import httpx
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---- Config ----
THAIPOST_API_KEY      = os.getenv("THAIPOST_API_KEY", "")
ETRACKINGS_API_URL    = "https://api.etrackings.com/api/v3/tracks/find"
ETRACKINGS_API_KEY    = os.getenv("ETRACKINGS_API_KEY", "")
ETRACKINGS_KEY_SECRET = os.getenv("ETRACKINGS_KEY_SECRET", "")
KEX_SCRAPER_URL       = os.getenv("KEX_SCRAPER_URL", "")   # Railway microservice URL
KEX_SCRAPER_KEY       = os.getenv("KEX_SCRAPER_KEY", "vela-kex-scraper")
SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
TOKEN_URL        = "https://trackapi.thailandpost.co.th/post/api/v1/authenticate/token"
TRACK_URL        = "https://trackapi.thailandpost.co.th/post/api/v1/track"

DONE_STATUSES = {"delivered", "returned"}

# ---- SMS Config ----
SMS_API_KEY    = os.getenv("SMS_API_KEY", "")
SMS_API_SECRET = os.getenv("SMS_API_SECRET", "")
SMS_SENDER     = "VeLA"

# SMS — ส่งเฉพาะตอนถึงเท่านั้น (ประหยัดเครดิต)
SMS_TEMPLATES = {
    "accepted":         "VeLA Cold Brew: ร้านได้จัดกาแฟของคุณแล้ว 📦 ติดตามพัสดุได้เลย: velacoldbrew.com/track/{barcode}",
    "in_transit":       None,  # ไม่ส่ง SMS
    "out_for_delivery": None,  # ไม่ส่ง SMS
    "delivered": "VeLA Cold Brew: พัสดุของคุณถึงแล้ว ✓ ขอบคุณที่สั่งซื้อนะคะ 🐰 สั่งซื้อและรับสิทธิพิเศษสมาชิกได้ที่: velacoldbrew.com",
    "returned": None,  # แจ้ง admin ผ่าน LINE
    "problem":  None,  # แจ้ง admin ผ่าน LINE
}

# LINE — ส่งทั้งตอนจัดส่งและตอนถึง (ฟรี)
LINE_TEMPLATES = {
    "accepted": "VeLA Cold Brew: ร้านได้จัดกาแฟของคุณแล้ว 📦 ติดตามพัสดุได้เลย: velacoldbrew.com/track/{barcode}",
    "in_transit":       None,
    "out_for_delivery": None,
    "delivered": "VeLA Cold Brew: พัสดุของคุณถึงแล้ว ✓ ขอบคุณที่สั่งซื้อนะคะ 🐰 สั่งซื้อและรับสิทธิพิเศษสมาชิกได้ที่: velacoldbrew.com",
    "returned": None,
    "problem":  None,
}


ADMIN_LINE_USER_ID = os.getenv("ADMIN_LINE_USER_ID", "U28d1b5573f79da2f3ff3f52ccc1fcf1c")
ALERT_STATUSES = {"returned", "problem"}

async def send_line_notify(line_user_id: str, message: str, barcode: str = "", status: str = "", customer: str = "", phone: str = ""):
    """ส่งข้อความผ่าน LINE OA"""
    token = os.getenv("LINE_CHANNEL_TOKEN", "")
    if not token:
        print("[LINE] ยังไม่ได้ตั้ง LINE_CHANNEL_TOKEN")
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.line.me/v2/bot/message/push",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"to": line_user_id, "messages": [{"type": "text", "text": message}]}
            )
            success = resp.status_code == 200
            if success:
                print(f"[LINE] ✓ ส่งหา {line_user_id[:8]}... สำเร็จ")
            else:
                print(f"[LINE] ✗ {resp.text}")

            # log ลง sms_logs
            if barcode and status:
                try:
                    sb = get_supabase()
                    sb.table("sms_logs").insert({
                        "barcode":      barcode,
                        "phone":        phone,
                        "customer":     customer,
                        "status":       status,
                        "message":      message,
                        "success":      success,
                        "notify_via":   "line",
                        "delivery_status": "sent" if success else "failed",
                    }).execute()
                except Exception as log_err:
                    print(f"[LINE] log error: {log_err}")
            return success
    except Exception as e:
        print(f"[LINE] ERROR: {e}")
        return False


async def send_sms(phone: str, message: str, barcode: str = "", status: str = "", customer: str = ""):
    """ส่ง SMS ผ่าน Thaibulksms พร้อม log"""
    print(f"[SMS] key={SMS_API_KEY[:6]}... secret={SMS_API_SECRET[:6]}...")
    if not SMS_API_KEY or not SMS_API_SECRET:
        print(f"[SMS] ยังไม่ได้ตั้ง SMS key")
        return False
    if not phone or len(phone) < 9:
        print(f"[SMS] เบอร์โทรไม่ถูกต้อง: {phone}")
        return False

    # เช็คว่าเคยส่ง status นี้ไปแล้วหรือยัง (เฉพาะกรณีมี barcode)
    if barcode and status:
        try:
            sb = get_supabase()
            existing = sb.table("sms_logs").select("id").eq("barcode", barcode).eq("status", status).execute()
            if existing.data:
                print(f"[SMS] ข้าม {barcode} → {status} (เคยส่งแล้ว)")
                return False
        except:
            pass

    success = False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api-v2.thaibulksms.com/sms",
                data={
                    "msisdn":  phone,
                    "message": message,
                    "sender":  SMS_SENDER,
                },
                headers={"content-type": "application/x-www-form-urlencoded"},
                auth=(SMS_API_KEY, SMS_API_SECRET),
            )
            data = resp.json()
            # Thaibulksms success = มี phone_number_list และไม่มี error
            success = (
                "error" not in data and
                len(data.get("phone_number_list", [])) > 0
            )
            message_id = data.get("phone_number_list", [{}])[0].get("message_id", "") if success else ""
            if success:
                credit_left = data.get("remaining_credit", "?")
                print(f"[SMS] ✓ ส่งไปที่ ...{phone[-4:]} สำเร็จ (เครดิตคงเหลือ: {credit_left})")
            else:
                print(f"[SMS] ✗ ส่งไม่สำเร็จ: {data}")
    except Exception as e:
        print(f"[SMS] ERROR: {e}")

    # Log ทุกการส่ง
    if barcode and status:
        try:
            sb = get_supabase()
            sb.table("sms_logs").insert({
                "barcode":         barcode,
                "phone":           phone,
                "customer":        customer,
                "status":          status,
                "message":         message,
                "success":         success,
                "message_id":      message_id,
                "notify_via":      "sms",
                "delivery_status": "sent" if success else "failed",
            }).execute()
        except Exception as e:
            print(f"[SMS] log error: {e}")

    return success


async def _notify_customer_line(sb, order_id: str, phone: str, customer: str, message: str, status_tag: str) -> bool:
    """แจ้งลูกค้าทาง LINE (หา line_user_id จากเบอร์) — ส่งเมื่อผูก LINE ไว้และไม่ได้ปิดแจ้งเตือน + เขียน log"""
    try:
        line_uid  = ""
        notify_ch = None
        if phone:
            look = sb.table("customers").select("line_user_id,notify_channel").eq("phone", phone).execute()
            if look.data:
                line_uid  = look.data[0].get("line_user_id") or ""
                notify_ch = look.data[0].get("notify_channel")
        channel = notify_ch or ("line" if line_uid else "sms")
        if channel == "line" and line_uid:
            await send_line_notify(line_uid, message, barcode=order_id, status=status_tag, customer=customer, phone=phone)
            return True
    except Exception as e:
        print(f"[customer-notify] {status_tag} error: {e}")
    return False

# ---- Token cache ----
_token_cache: dict = {"token": None, "expires_at": 0}

# ---- Supabase client ----
def get_supabase() -> Client:
    """ใช้ service role key เพื่อ bypass RLS (backend only)"""
    key = SUPABASE_SERVICE_KEY if SUPABASE_SERVICE_KEY else SUPABASE_KEY
    return create_client(SUPABASE_URL, key)

# ---- Status mapping ----
STATUS_MAP = {
    "101": ("accepted",          "รับฝากแล้ว"),
    "102": ("accepted",          "รับฝากแล้ว"),
    "103": ("accepted",          "รับฝากแล้ว"),
    "201": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "202": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "203": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "204": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "205": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "206": ("in_transit",        "ถึงที่ทำการไปรษณีย์"),
    "207": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "208": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "209": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "210": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "211": ("in_transit",        "รับเข้าศูนย์คัดแยก"),
    "212": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "301": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "302": ("in_transit",        "อยู่ระหว่างขนส่ง"),
    "303": ("delivered",         "ผู้รับมารับเอง"),
    "304": ("problem",           "ติดต่อผู้รับไม่ได้"),
    "305": ("returned",          "ผู้รับปฏิเสธการรับ"),  # นำจ่ายไม่สำเร็จ - ปฏิเสธรับ
    "306": ("returned",          "ส่งคืนต้นทาง"),         # ส่งคืนต้นทาง
    "307": ("returned",          "ผู้รับปฏิเสธการรับ"),
    "401": ("out_for_delivery",  "ออกนำจ่ายแล้ว"),
    "402": ("out_for_delivery",  "ออกนำจ่ายแล้ว"),
    "501": ("delivered",         "จัดส่งสำเร็จ"),
    "502": ("delivered",         "จัดส่งสำเร็จ"),
    "503": ("delivered",         "จัดส่งสำเร็จ"),
    "504": ("delivered",         "จัดส่งสำเร็จ"),
    "600": ("returned",          "ตีกลับ"),
    "601": ("returned",          "ตีกลับ"),
    "602": ("returned",          "ตีกลับ"),
    "603": ("returned",          "ส่งคืนต้นทาง"),
    "700": ("problem",           "มีปัญหา"),
    "701": ("problem",           "มีปัญหา"),
}

def map_status(code: str):
    code = str(code).strip()
    if not code:
        return ("pending", "รอข้อมูล")
    return STATUS_MAP.get(code, ("unknown", f"ไม่ทราบสถานะ ({code})"))


# ---- Thailand Post helpers ----
async def get_access_token() -> str:
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            TOKEN_URL,
            headers={"Authorization": f"Token {THAIPOST_API_KEY}", "Content-Type": "application/json"},
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"ขอ token ไม่สำเร็จ: {resp.status_code} — {resp.text}")
        data = resp.json()
        token = data.get("token")
        if not token:
            raise HTTPException(status_code=502, detail=f"ไม่ได้รับ token: {data}")
        _token_cache["token"] = token
        _token_cache["expires_at"] = now + 3600
        return token


def detect_carrier(barcode: str) -> str:
    """ตรวจสอบ 'ผู้ให้บริการ tracking' (สำหรับ route API เช็คสถานะ) จาก format เลข tracking"""
    b = (barcode or "").upper()
    # Flash Express (Shopee-managed) ใช้เลข THxxxxxxxxxxC — KEX ใช้ SXF อย่างเดียวแล้ว
    if re.match(r'^TH', b):                 return "flash-express"
    if re.match(r'^(SXF|SCPK)', b):        return "kex-express"
    if re.match(r'^(FLE|FEX)', b):         return "flash-express"
    if re.match(r'^(TDE|JPT|JTTH)', b):    return "jt-express"
    if re.match(r'^SCG', b):               return "scg-express"
    return "thailand_post"


def business_carrier(tracking: str, chosen: str = "") -> str:
    """แปลงเลข tracking → 'ชื่อขนส่งจริง' สำหรับเก็บ/แสดง (Flash Express / KEX Express / POST SABUY / Seller Own Fleet)"""
    t = (tracking or "").upper().strip()
    if t.startswith("TH"):    return "Flash Express"   # Shopee-managed THxxxxC
    if t.startswith("SXF"):   return "KEX Express"
    if t.startswith("JM"):    return "POST SABUY"
    if t in ("", "-"):        return chosen or "Seller Own Fleet"
    return chosen or "POST SABUY"


async def fetch_etrackings(barcode: str, courier: str) -> dict:
    """ดึงสถานะพัสดุจาก eTrackings API (Kerry, Flash, J&T)"""
    ETRACK_STATUS_MAP = {
        "ON_DELIVERED":    ("delivered",         "จัดส่งสำเร็จ"),
        "ON_SHIPPING":     ("out_for_delivery",   "กำลังจัดส่ง"),
        "ON_PICKED_UP":    ("accepted",           "รับฝากแล้ว"),
        "ON_TRANSIT":      ("in_transit",         "อยู่ระหว่างขนส่ง"),
        "ON_OTHER_STATUS": ("in_transit",         "อยู่ระหว่างขนส่ง"),
        "ON_RETURNED":     ("returned",           "ส่งคืนต้นทาง"),
        "ON_FAILED":       ("problem",            "จัดส่งไม่สำเร็จ"),
        "ON_EXCEPTION":    ("problem",            "มีปัญหา"),
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.post(
                "https://api.etrackings.com/api/v3/tracks/find",
                headers={
                    "Content-Type":          "application/json",
                    "Etrackings-Api-Key":    ETRACKINGS_API_KEY,
                    "Etrackings-Key-Secret": ETRACKINGS_KEY_SECRET,
                    "Accept-Language":       "th",
                },
                json={"courier": courier, "trackingNo": barcode},
            )
            body = res.json()
            if res.status_code != 200 or not body.get("data"):
                print(f"[eTrackings] {barcode} → {body.get('meta', {})}")
                return {"barcode": barcode, "status": "unknown", "status_th": "ไม่พบข้อมูล"}

            track = body["data"]
            raw_status = track.get("status", "")
            status, status_th = ETRACK_STATUS_MAP.get(raw_status, ("in_transit", track.get("currentStatus", raw_status)))

            # สร้าง events จาก timelines — eTrackings ส่งมาเรียงจากเก่าไปใหม่อยู่แล้ว
            events = []
            for day in (track.get("timelines") or []):
                for detail in (day.get("details") or []):
                    events.append({
                        "datetime":    detail.get("dateTime", ""),
                        "description": detail.get("description", ""),
                        "status_th":   detail.get("description", ""),
                    })

            return {
                "barcode":         barcode,
                "status":          status,
                "status_th":       status_th,
                "latest_location": track.get("currentStatus", ""),
                "events":          events,
            }
    except Exception as e:
        print(f"[eTrackings] error {barcode}: {e}")
        return {"barcode": barcode, "status": "error", "status_th": "เชื่อมต่อไม่ได้"}


def _event_sort_key(ev: dict):
    """คีย์เรียง event ตามเวลาจริง (เก่า→ใหม่) — parse รูปแบบไปรษณีย์ไทย 'DD/MM/YYYY HH:MM:SS+TZ' (ปี พ.ศ.) และ ISO
    ปีเป็น พ.ศ. หรือ ค.ศ. ไม่สำคัญต่อการเรียง เพราะ monotonic เหมือนกัน"""
    dt = (ev.get("datetime") or "").strip()
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?', dt)
    if m:
        d, mo, y, hh, mm, ss = m.groups()
        return (int(y), int(mo), int(d), int(hh), int(mm), int(ss or 0))
    m2 = re.search(r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?', dt)
    if m2:
        y, mo, d, hh, mm, ss = m2.groups()
        return (int(y), int(mo), int(d), int(hh), int(mm), int(ss or 0))
    return (0, 0, 0, 0, 0, 0)  # ไม่รู้เวลา → ถือว่าเก่าสุด


async def fetch_tracking_batch(barcodes: list) -> list:
    """เรียก Thailand Post API แบบ batch สูงสุด 20 เลขต่อครั้ง"""
    token = await get_access_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TRACK_URL,
            json={"status": "all", "language": "TH", "barcode": barcodes},
            headers={"Authorization": f"Token {token}", "Content-Type": "application/json"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"ไปรษณีไทย API: HTTP {resp.status_code} — {resp.text}")
    data = resp.json()
    if not data.get("status"):
        raise HTTPException(status_code=502, detail=data.get("message", "API error"))
    response    = data.get("response", {})
    items       = response.get("items", {})
    track_count = response.get("track_count", {})

    results = []
    for barcode in barcodes:
        events_raw = items.get(barcode, [])
        events = []
        for e in events_raw:
            code = str(e.get("status", "")).strip()
            status, desc_th = map_status(code) if code else ("pending", "รอข้อมูล")
            events.append({
                "status_code":  code,
                "status":       status,
                "description":  e.get("status_description") or desc_th,
                "datetime":     e.get("status_date"),
                "location":     e.get("location"),
            })
        # เรียงตามเวลาจริง (เก่า→ใหม่) แทนการ reverse แบบเดาลำดับ — กันเคส API ส่งลำดับสลับ
        # (บั๊กเดิม: บาง parcel API ส่งเก่า→ใหม่ พอ reverse กลายเป็นใหม่→เก่า ทำให้ latest ชี้ไป event รับฝาก)
        events.sort(key=_event_sort_key)
        latest = events[-1] if events else None
        # ถ้ามี delivered ใน events ใดๆ → ใช้ delivered เสมอ
        # เพราะไปรษณีย์บางทีบันทึก "ติดต่อไม่ได้" พร้อมกับ "นำจ่ายสำเร็จ" ห่างกันแค่วินาทีเดียว
        has_delivered = any(e["status"] in DONE_STATUSES for e in events)
        if has_delivered:
            delivered_event = next((e for e in reversed(events) if e["status"] in DONE_STATUSES), None)
            current_status = delivered_event["status"] if delivered_event else "delivered"
            current_status_th = delivered_event["description"] if delivered_event else "จัดส่งสำเร็จ"
            if delivered_event:
                latest = delivered_event  # การ์ดบนโชว์ location/เวลา ของ event นำจ่ายสำเร็จ ให้ตรงกับสถานะ
        else:
            current_status, current_status_th = map_status(latest["status_code"] if latest else "")
        results.append({
            "barcode":           barcode,
            "status":            current_status,
            "status_th":         current_status_th,
            "latest_event":      latest,
            "events":            events,
            "track_count_today": track_count.get("count_number"),
            "track_count_limit": track_count.get("track_count_limit"),
        })
    return results


async def fetch_tracking(barcode: str) -> dict:
    """ดึงสถานะพัสดุ — route อัตโนมัติตาม carrier"""
    carrier = detect_carrier(barcode)
    if carrier == "kex-express":
        # Kerry → ลอง Railway scraper ก่อน ถ้าไม่มีค่อย eTrackings
        if KEX_SCRAPER_URL:
            try:
                async with httpx.AsyncClient(timeout=35) as client:
                    r = await client.get(
                        f"{KEX_SCRAPER_URL}/track/{barcode}",
                        headers={"x-api-key": KEX_SCRAPER_KEY},
                    )
                    if r.status_code == 200:
                        data = r.json()
                        if data.get("status") not in ("unknown", "error", None):
                            return data
            except Exception as e:
                print(f"[KEX Scraper] error {barcode}: {e}")
        # ไม่มี fallback — ถ้า scraper ไม่ได้ผล return unknown
        return {"barcode": barcode, "status": "unknown", "status_th": "ไม่พบข้อมูล", "events": []}
    elif carrier == "flash-express":
        # Flash (Shopee-managed) — ใช้ scraper เดียวกับ KEX (headless เปิดหน้า Flash ผ่าน 5 Second Shield)
        if KEX_SCRAPER_URL:
            try:
                async with httpx.AsyncClient(timeout=75) as client:
                    r = await client.get(
                        f"{KEX_SCRAPER_URL}/track-flash/{barcode}",
                        headers={"x-api-key": KEX_SCRAPER_KEY},
                    )
                    if r.status_code == 200:
                        data = r.json()
                        # รับผลถ้าไม่ใช่ error/unknown (pending = ยังไม่เข้าระบบ ก็คืนได้)
                        if data.get("status") not in ("error", "unknown", None):
                            return data
            except Exception as e:
                print(f"[Flash Scraper] error {barcode}: {e}")
        # scraper ล่ม/ยังไม่ได้ตั้ง → placeholder ไม่ให้ระบบพัง
        return {"barcode": barcode, "status": "shopee_managed",
                "status_th": "จัดส่งโดย Flash Express (Shopee) — ติดตามในแอป Shopee",
                "events": []}
    elif carrier != "thailand_post" and ETRACKINGS_API_KEY:
        return await fetch_etrackings(barcode, carrier)
    return (await fetch_tracking_batch([barcode]))[0]


async def run_cron():
    """เช็คเฉพาะพัสดุที่ is_done = false ทุก 3 ชั่วโมง เฉพาะช่วง 10:00-18:00"""

    print("[cron] เริ่มเช็คสถานะพัสดุที่ยังไม่เสร็จ...")
    sb = get_supabase()

    # ลบ order เว็บที่รอชำระเกินเวลา — ใช้ created_at ถ้ามี ไม่งั้น fallback ไป order_date
    # (บั๊กเดิม: order เก่า created_at เป็น NULL ตัวกรอง .lt(created_at) เลยไม่เจอ = ไม่ลบ)
    try:
        expire_hours = int(os.getenv("ORDER_EXPIRE_HOURS", "48"))
        now      = datetime.utcnow()
        cutoff   = now - timedelta(hours=expire_hours)
        # fallback ด้วย order_date (มีแค่วันที่) → เผื่อ 1 วันกันลบเร็วไป
        date_cutoff = (now - timedelta(hours=expire_hours) - timedelta(days=1)).date()

        pending = sb.table("orders") \
            .select("order_id,created_at,order_date") \
            .eq("status", "รอชำระเงิน") \
            .eq("channel", "web") \
            .execute()

        expired_ids = []
        for r in (pending.data or []):
            ca = r.get("created_at")
            od = r.get("order_date")
            old = False
            if ca:
                try:
                    cadt = datetime.fromisoformat(str(ca).replace("Z", "+00:00")).replace(tzinfo=None)
                    old = cadt < cutoff
                except Exception:
                    old = False
            elif od:  # ไม่มี created_at → ใช้ order_date
                try:
                    old = datetime.strptime(str(od)[:10], "%Y-%m-%d").date() <= date_cutoff
                except Exception:
                    old = False
            if old:
                expired_ids.append(r["order_id"])

        if expired_ids:
            sb.table("orders").delete().in_("order_id", expired_ids).execute()
            # ลบแถวบัญชีของ order ที่ค้างชำระด้วย ไม่ให้รายรับค้างในบัญชี
            try:
                sb.table("accounting").delete().in_("order_id", expired_ids).execute()
            except Exception as e:
                print(f"[cron] ลบ accounting ค้างชำระ error: {e}")
            print(f"[cron] ลบ order ค้างชำระหมดเวลา {len(expired_ids)} รายการ: {expired_ids}")
        else:
            print(f"[cron] ไม่มี order ค้างชำระหมดเวลา (รอชำระ web ทั้งหมด {len(pending.data or [])} รายการ)")
    except Exception as e:
        print(f"[cron] expire error: {e}")

    rows = sb.table("shipments").select("barcode").eq("is_done", False).execute()
    barcodes = [r["barcode"] for r in (rows.data or [])]
    print(f"[cron] พบ {len(barcodes)} รายการที่ต้องเช็ค")

    # แยก barcode ตามขนส่ง
    thaipost_barcodes = [b for b in barcodes if detect_carrier(b) == "thailand_post"]
    kex_barcodes      = [b for b in barcodes if detect_carrier(b) == "kex-express"]
    flash_barcodes    = [b for b in barcodes if detect_carrier(b) == "flash-express"]
    other_barcodes    = [b for b in barcodes if detect_carrier(b) not in ("thailand_post", "kex-express", "flash-express")]
    etracking_barcodes = other_barcodes
    print(f"[cron] ไปรษณีย์ไทย: {len(thaipost_barcodes)}, KEX: {len(kex_barcodes)}, Flash: {len(flash_barcodes)}, eTrackings: {len(etracking_barcodes)}")

    # เช็ค eTrackings เฉพาะหลัง 13:00 น. (ประหยัด credit)
    thai_hour = (datetime.utcnow().hour + 7) % 24
    if thai_hour < 13:
        print(f"[cron] ข้าม eTrackings — เวลา {thai_hour}:xx น. ยังไม่ถึงบ่ายโมง")
        etracking_barcodes = []
    # KEX ใช้ scraper ฟรี เช็คได้ตลอดเวลา

    # เช็คเฉพาะเลขที่ไม่เกิน 7 วัน (KEX + Flash ใช้ scraper ฟรี, eTrackings เสีย credit)
    if kex_barcodes or flash_barcodes or etracking_barcodes:
        cutoff_7d = (datetime.utcnow() - timedelta(days=7)).isoformat()
        try:
            all_third_party = kex_barcodes + flash_barcodes + etracking_barcodes
            new_rows = sb.table("shipments") \
                .select("barcode") \
                .in_("barcode", all_third_party) \
                .gte("created_at", cutoff_7d) \
                .execute()
            filtered = {r["barcode"] for r in (new_rows.data or [])}
            kex_barcodes       = [b for b in kex_barcodes if b in filtered]
            flash_barcodes     = [b for b in flash_barcodes if b in filtered]
            etracking_barcodes = [b for b in etracking_barcodes if b in filtered]
            print(f"[cron] หลังกรอง 7 วัน: KEX {len(kex_barcodes)}, Flash {len(flash_barcodes)}, eTrackings {len(etracking_barcodes)} รายการ")
        except Exception as e:
            print(f"[cron] filter error: {e}")

    # ดึง status เก่าทั้งหมดก่อน
    old_rows = sb.table("shipments").select("barcode,status").in_("barcode", barcodes).execute()
    old_status_map = {r["barcode"]: r["status"] for r in (old_rows.data or [])}

    all_results = []

    # เช็ค KEX ผ่าน Fly.io scraper — แบ่งทีละ 5 เลข ป้องกัน timeout
    batch_size = 5
    for i in range(0, len(kex_barcodes), batch_size):
        batch = kex_barcodes[i:i+batch_size]
        if not KEX_SCRAPER_URL:
            break
        try:
            async with httpx.AsyncClient(timeout=200) as client:
                r = await client.post(
                    f"{KEX_SCRAPER_URL}/track/bulk",
                    headers={"x-api-key": KEX_SCRAPER_KEY},
                    json={"barcodes": batch},
                )
                if r.status_code == 200:
                    bulk_data = r.json().get("results", {})
                    for b in batch:
                        res = bulk_data.get(b, {"status": "unknown", "status_th": "ไม่พบข้อมูล", "events": []})
                        all_results.append({
                            "barcode":      b,
                            "status":       res.get("status", "unknown"),
                            "status_th":    res.get("status_th", ""),
                            "latest_event": {"location": res.get("latest_location", ""), "datetime": (res.get("events") or [{}])[-1].get("datetime", "")},
                            "events":       res.get("events", []),
                        })
                        print(f"[cron] KEX {b} → {res.get('status')}")
                else:
                    print(f"[KEX Scraper] bulk error: HTTP {r.status_code}")
        except Exception as e:
            print(f"[KEX Scraper] bulk error batch {i}: {e}")

    # เช็ค Flash ผ่าน scraper เดียวกัน (headless เปิดหน้า Flash ผ่าน 5 Second Shield) — ฟรี เช็คได้ตลอด
    for i in range(0, len(flash_barcodes), batch_size):
        batch = flash_barcodes[i:i+batch_size]
        if not KEX_SCRAPER_URL:
            break
        try:
            async with httpx.AsyncClient(timeout=200) as client:
                r = await client.post(
                    f"{KEX_SCRAPER_URL}/track-flash/bulk",
                    headers={"x-api-key": KEX_SCRAPER_KEY},
                    json={"pnos": batch},
                )
                if r.status_code == 200:
                    bulk_data = r.json().get("results", {})
                    for b in batch:
                        res = bulk_data.get(b, {"status": "unknown", "status_th": "ไม่พบข้อมูล", "events": []})
                        all_results.append({
                            "barcode":      b,
                            "status":       res.get("status", "unknown"),
                            "status_th":    res.get("status_th", ""),
                            "latest_event": {"location": res.get("latest_location", ""), "datetime": (res.get("events") or [{}])[-1].get("datetime", "")},
                            "events":       res.get("events", []),
                        })
                        print(f"[cron] Flash {b} → {res.get('status')}")
                else:
                    print(f"[Flash Scraper] bulk error: HTTP {r.status_code}")
        except Exception as e:
            print(f"[Flash Scraper] bulk error batch {i}: {e}")

    # เช็ค J&T/อื่นๆ ผ่าน eTrackings
    for b in etracking_barcodes:
        try:
            res = await fetch_etrackings(b, detect_carrier(b))
            all_results.append({
                "barcode":      b,
                "status":       res.get("status", "unknown"),
                "status_th":    res.get("status_th", ""),
                "latest_event": {"location": res.get("latest_location", ""), "datetime": (res.get("events") or [{}])[0].get("datetime", "")},
                "events":       res.get("events", []),
            })
            print(f"[cron] eTrackings {b} → {res.get('status')}")
        except Exception as e:
            print(f"[cron] eTrackings error {b}: {e}")

    # เช็ค Thailand Post แบบ batch ทีละ 20 เลข
    batch_size = 20
    for i in range(0, len(thaipost_barcodes), batch_size):
        batch = thaipost_barcodes[i:i+batch_size]
        # batch ถูก set ใน new structure แล้ว
        try:
            results = await fetch_tracking_batch(batch)
        except Exception as e:
            print(f"[cron] batch ERROR: {e} — รอ 3 นาทีแล้วลองใหม่...")
            await asyncio.sleep(180)
            try:
                results = await fetch_tracking_batch(batch)
                print(f"[cron] retry สำเร็จ")
            except Exception as e2:
                print(f"[cron] batch retry ERROR: {e2} — รอ 3 นาทีอีกครั้ง...")
                await asyncio.sleep(180)
                try:
                    results = await fetch_tracking_batch(batch)
                    print(f"[cron] retry ครั้งที่ 2 สำเร็จ")
                except Exception as e3:
                    print(f"[cron] หยุดเช็ค batch นี้: {e3}")
                    continue

        all_results.extend(results)

    # process ทุก results รวมกัน (Thailand Post + eTrackings)
    for result in all_results:
        barcode    = result["barcode"]
        status     = result["status"]
        is_done    = status in DONE_STATUSES
        latest     = result.get("latest_event") or {}
        old_status = old_status_map.get(barcode, "pending")

        # ถ้า API ส่ง events ว่างมา (Thailand Post มีปัญหา) → ข้ามไปไม่ทับสถานะเดิม
        if not result.get("events") and status == "pending":
            print(f"[cron] {barcode} → ข้าม (API ไม่มีข้อมูล)")
            continue

        sb.table("shipments").update({
            "status":          status,
            "status_th":       result["status_th"],
            "latest_location": latest.get("location"),
            "latest_datetime": latest.get("datetime"),
            "is_done":         is_done,
            "last_checked_at": datetime.utcnow().isoformat(),
        }).eq("barcode", barcode).execute()
        print(f"[cron] {barcode} → {status} {'✓ done' if is_done else ''}")

        # เชื่อมสถานะกลับไปที่ orders — พัสดุถึง/ตีกลับแล้ว → อัปเดต order ให้ตรงกัน (link statuses)
        if is_done:
            try:
                sr = sb.table("shipping").select("order_id").eq("tracking", barcode).execute()
                if sr.data:
                    order_new_status = "ตีกลับ" if status == "returned" else "จัดส่งสำเร็จ"
                    sb.table("orders").update({"status": order_new_status}).eq("order_id", sr.data[0]["order_id"]).execute()
                    print(f"[cron] link → order {sr.data[0]['order_id']} = {order_new_status}")
            except Exception as e:
                print(f"[cron] link order status error: {e}")

        # แจ้งเตือนถ้าสถานะเปลี่ยน
        # ถ้า old_status เป็น pending และ status ใหม่เป็น in_transit หรือสูงกว่า → ส่ง SMS accepted แทน
        sms_status = status
        if old_status in ("pending",) and status in ("in_transit", "out_for_delivery") and not SMS_TEMPLATES.get(status):
            sms_status = "accepted"
        # เลือก template ตาม channel
        line_msg = LINE_TEMPLATES.get(sms_status)
        sms_msg  = SMS_TEMPLATES.get(sms_status)
        has_notify = (line_msg or sms_msg)
        if status != old_status and has_notify:
            msg = line_msg or sms_msg
            ship_row = sb.table("shipping").select("order_id").eq("tracking", barcode).execute()
            if ship_row.data:
                order_id  = ship_row.data[0]["order_id"]
                order_row = sb.table("orders").select("phone,customer").eq("order_id", order_id).execute()
                if order_row.data:
                    phone    = order_row.data[0].get("phone", "")
                    customer = order_row.data[0].get("customer", "")
                    if phone:
                        # เช็ค notify_channel จาก customers table
                        notify       = "sms"
                        line_uid     = ""
                        try:
                            cust = sb.table("customers").select("notify_channel,line_user_id").eq("phone", phone).execute()
                            if cust.data:
                                line_uid = cust.data[0].get("line_user_id") or ""
                                # ลูกค้าที่ผูก LINE ไว้ → default รับแจ้งเตือนทาง LINE (เคารพค่าที่ตั้งเองไว้ก่อน)
                                notify   = cust.data[0].get("notify_channel") or ("line" if line_uid else "sms")
                        except:
                            pass

                        final_msg = msg.replace("{barcode}", barcode)

                        if notify == "none":
                            print(f"[notify] ข้าม {customer} → ปิดแจ้งเตือน")
                        elif notify == "line" and line_uid:
                            # LINE ใช้ LINE_TEMPLATES (มีทั้ง accepted และ delivered)
                            if line_msg:
                                final_line_msg = line_msg.replace("{barcode}", barcode)
                                await send_line_notify(line_uid, final_line_msg, barcode=barcode, status=sms_status, customer=customer, phone=phone)
                                print(f"[LINE] แจ้ง {customer} → {status}")
                            else:
                                print(f"[LINE] ข้าม {customer} → ไม่มี LINE template สำหรับ {sms_status}")
                        else:
                            # SMS ใช้ SMS_TEMPLATES (เฉพาะ delivered)
                            if sms_msg:
                                final_sms_msg = sms_msg.replace("{barcode}", barcode)
                                await send_sms(phone, final_sms_msg, barcode=barcode, status=sms_status, customer=customer)
                                print(f"[SMS] แจ้ง {customer} ({phone[-4:].zfill(4)}) → {status}")
                            else:
                                print(f"[SMS] ข้าม {customer} → ไม่มี SMS template สำหรับ {sms_status}")

                        # แจ้ง admin ถ้ามีปัญหา
                        if status in ALERT_STATUSES and ADMIN_LINE_USER_ID:
                            admin_msg = f"⚠ VeLA Alert: พัสดุ {barcode} ของ {customer} ({phone}) สถานะ: {status_th or status}"
                            await send_line_notify(ADMIN_LINE_USER_ID, admin_msg)
                            print(f"[ADMIN] แจ้ง admin → {barcode} {status}")

        if thaipost_barcodes and i + batch_size < len(thaipost_barcodes):
            await asyncio.sleep(1)

    print("[cron] เสร็จแล้ว")


# ---- Scheduler ----
scheduler = AsyncIOScheduler()

RENDER_URL = os.getenv("RENDER_URL", "")

async def keep_alive():
    """Ping ตัวเองทุก 10 นาที ไม่ให้ Render sleep"""
    if not RENDER_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.get(f"{RENDER_URL}/health")
        print("[keep-alive] ping OK")
    except Exception as e:
        print(f"[keep-alive] ping failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(run_cron,   "interval", hours=3,    id="tracking_cron")
    scheduler.add_job(keep_alive, "interval", minutes=10, id="keep_alive")
    scheduler.start()
    print("[scheduler] cron started — ทุก 3 ชั่วโมง, keep-alive ทุก 10 นาที")
    yield
    scheduler.shutdown()

app = FastAPI(title="VeLA Tracking API", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://velacoldbrew.com",
        "https://www.velacoldbrew.com",
        "https://vela-web-sigma.vercel.app",
        "http://localhost:3000",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# DooSlip — LINE slip bot (รวมจาก service dooslip เดิม) mount ที่ /dooslip
# webhook @dooslip ให้ชี้มาที่ https://vela-tracking.onrender.com/dooslip/webhook
try:
    from dooslip_router import router as dooslip_router
    app.include_router(dooslip_router, prefix="/dooslip")
except Exception as e:
    print(f"[dooslip] ไม่โหลด router: {e}")


# ---- Models ----
class AddShipmentsRequest(BaseModel):
    barcodes: list[str]

class BulkRequest(BaseModel):
    barcodes: list[str]


# ---- Endpoints ----

@app.get("/health")
async def health():
    return {"status": "ok", "service": "VeLA Tracking API v2"}


@app.get("/products")
async def get_products():
    """ดึงสินค้าทั้งหมด พร้อมราคาหลังลด"""
    sb = get_supabase()
    res = sb.table("products").select("*").eq("active", True).order("sort_order").execute()
    products = []
    for p in (res.data or []):
        price      = int(p.get("price") or 0)
        disc_pct   = int(p.get("discount_pct") or 0)
        disc_price = round(price * (1 - disc_pct / 100))
        products.append({
            **p,
            "price":          price,
            "price_original": price,
            "price_discounted": disc_price,
            "discount_pct":   disc_pct,
            "discount_amount": price - disc_price,
        })
    return {"products": products}


# ── โปรลูกค้าใหม่: ลด 50% ของบิล เพดาน ฿130 (ออเดอร์แรกในระบบเว็บ ผูกกับเบอร์) ──
# config ผ่าน env — ปิด/เปิดโปรได้โดยไม่ต้องแก้โค้ด
def _promo_first_order_enabled() -> bool:
    return os.getenv("PROMO_FIRST_ORDER_50", "1").strip().lower() in ("1", "true", "yes", "on")

FIRST_ORDER_PCT = float(os.getenv("FIRST_ORDER_DISCOUNT_PCT", "50")) / 100.0   # 0.5
FIRST_ORDER_CAP = float(os.getenv("FIRST_ORDER_DISCOUNT_CAP", "130"))          # เพดานบาท

def _is_first_order_eligible(sb, phone: str) -> bool:
    """มีสิทธิ์ = (1) ยังไม่เคยกดใช้ส่วนลด และ (2) ยังไม่เคยมีออเดอร์ในระบบเว็บ (Shopee ไม่นับ)
    ต้องเช็คทั้งสองอย่างเสมอ — กัน guest/คนเคยสั่งแล้วมารับสิทธิ์ซ้ำ"""
    phone = (phone or "").strip()
    if not phone:
        return False
    cust = sb.table("customers").select("first_order_used").eq("phone", phone).execute()
    if cust.data and cust.data[0].get("first_order_used"):
        return False  # เคยใช้สิทธิ์ไปแล้ว
    orders = sb.table("orders").select("order_id").eq("phone", phone).eq("channel", "web").limit(1).execute()
    return len(orders.data or []) == 0  # ต้องไม่เคยมีออเดอร์เว็บมาก่อน

def _first_order_discount(subtotal: float) -> int:
    """ยอดส่วนลดจริง (บาท) = min(subtotal*50%, เพดาน) — สูตรเดียวกับฝั่ง frontend (promo.ts)"""
    if subtotal <= 0:
        return 0
    return int(min(round(subtotal * FIRST_ORDER_PCT), FIRST_ORDER_CAP))


@app.get("/products/check-first-order")
async def check_first_order(phone: str):
    """เช็คว่าลูกค้าเบอร์นี้มีสิทธิ์ส่วนลด 50% (สั่งครั้งแรกในระบบเว็บ) ไหม"""
    sb = get_supabase()
    if not _promo_first_order_enabled():
        return {"eligible": False, "discount_pct": 0}
    eligible = _is_first_order_eligible(sb, phone)
    return {"eligible": eligible, "discount_pct": int(FIRST_ORDER_PCT * 100) if eligible else 0}


@app.post("/admin/products/{product_id}")
async def update_product(product_id: int, body: dict, x_api_key: str = Header(default="")):
    """Admin — อัปเดตราคา/รายละเอียดสินค้า"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    allowed = {"name", "description", "flavor", "roast", "process", "price", "discount_pct", "image_url", "active"}
    update_data = {k: v for k, v in body.items() if k in allowed}
    if not update_data:
        raise HTTPException(status_code=400, detail="no valid fields")
    sb.table("products").update(update_data).eq("id", product_id).execute()
    return {"success": True}


@app.get("/track/{barcode}")
async def track_single(barcode: str):
    """เช็คสถานะพัสดุ 1 ชิ้น (real-time)"""
    return await fetch_tracking(barcode.upper().strip())


@app.post("/track/bulk")
async def track_bulk(body: BulkRequest):
    """เช็คสถานะพัสดุหลายชิ้นพร้อมกัน (real-time, สูงสุด 20)
    ตรวจสอบก่อนว่าเลข tracking อยู่ในระบบร้านไหม เพื่อป้องกันการเช็คเลขของคนอื่น
    """
    barcodes = [b.upper().strip() for b in body.barcodes if b.strip()]
    if not barcodes:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcodes")
    if len(barcodes) > 20:
        raise HTTPException(status_code=400, detail="ส่งได้สูงสุด 20 เลขต่อครั้ง")

    # เช็คก่อนว่าเลขเหล่านี้อยู่ในระบบร้านไหม (shipments หรือ shipping table)
    sb = get_supabase()
    barcodes_str = ",".join(f'"{b}"' for b in barcodes)

    # ยกเว้น Kerry/Flash/J&T/SCG จาก validation — เรียก eTrackings ได้เลยไม่ต้องอยู่ใน DB
    def is_third_party(b: str) -> bool:
        return bool(re.match(r'^(TH|SCPK|SXF|FLE|FEX|TDE|JPT|JTTH|SCG)', b, re.I))

    third_party = [b for b in barcodes if is_third_party(b)]
    need_check  = [b for b in barcodes if not is_third_party(b)]

    valid_barcodes = set(third_party)  # third-party ผ่านได้เลย
    try:
        if need_check:
            # เช็คจาก shipments table (cron tracking)
            res1 = sb.table("shipments").select("barcode").in_("barcode", need_check).execute()
            for row in (res1.data or []):
                valid_barcodes.add(row["barcode"].upper())

            # เช็คจาก shipping table (เลข tracking จาก admin)
            res2 = sb.table("shipping").select("tracking").in_("tracking", need_check).execute()
            for row in (res2.data or []):
                if row.get("tracking"):
                    valid_barcodes.add(row["tracking"].upper())
    except Exception as e:
        print(f"[track/bulk] เช็ค DB error: {e}")
        valid_barcodes = set(barcodes)

    output = []
    valid_to_fetch = []
    for b in barcodes:
        if b in valid_barcodes:
            valid_to_fetch.append(b)
        else:
            # เลขไม่อยู่ในระบบร้าน — ไม่เรียก API ขนส่ง
            output.append({
                "barcode": b,
                "status": "not_found",
                "status_th": "ไม่พบเลขพัสดุในระบบ",
            })

    if valid_to_fetch:
        tasks   = [fetch_tracking(b) for b in valid_to_fetch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for barcode, result in zip(valid_to_fetch, results):
            if isinstance(result, Exception):
                output.append({"barcode": barcode, "status": "error", "error": str(result)})
            else:
                output.append(result)

    return {"results": output, "total": len(output)}


@app.post("/shipments/add")
async def add_shipments(body: AddShipmentsRequest):
    """เพิ่ม tracking numbers เข้า database (cron จะเช็คอัตโนมัติ)"""
    barcodes = [b.upper().strip() for b in body.barcodes if b.strip()]
    if not barcodes:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcodes")
    sb = get_supabase()
    rows = [{"barcode": b, "status": "pending", "is_done": False} for b in barcodes]
    # upsert — ถ้ามีอยู่แล้วไม่ทับ
    sb.table("shipments").upsert(rows, on_conflict="barcode", ignore_duplicates=True).execute()
    return {"added": len(barcodes), "barcodes": barcodes}


@app.get("/shipments")
async def list_shipments(is_done: Optional[bool] = None):
    """ดูรายการพัสดุทั้งหมด กรองด้วย ?is_done=false หรือ ?is_done=true"""
    sb    = get_supabase()
    query = sb.table("shipments").select("*").order("created_at", desc=True)
    if is_done is not None:
        query = query.eq("is_done", is_done)
    rows = query.execute()
    return {"shipments": rows.data, "total": len(rows.data or [])}


@app.post("/shipments/check-now")
async def check_now():
    """trigger cron ทันที ไม่ต้องรอ 3 ชั่วโมง"""
    await run_cron()
    return {"message": "กำลังเช็คสถานะ... ดูผลได้ที่ /shipments"}


# ---- Import Excel ----
from fastapi import UploadFile, File
import io
import pandas as pd

def safe_date(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except:
        pass
    try:
        ts = pd.to_datetime(v, dayfirst=True)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    except:
        return None

def safe_val(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except:
        pass
    return v

class TestSMSRequest(BaseModel):
    phone: str
    message: str = "VeLA Cold Brew: ทดสอบระบบ SMS ✓"

# ---- Admin Auth ----
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

def check_admin_key(request_key: str):
    """ตรวจสอบ admin API key"""
    if ADMIN_API_KEY and request_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


class TestLineRequest(BaseModel):
    line_user_id: str
    message: str = "VeLA Cold Brew: ทดสอบ LINE notification 🐰"

@app.post("/admin/test-line")
async def test_line(body: TestLineRequest, x_api_key: str = Header(default="")):
    """ทดสอบส่ง LINE message"""
    check_admin_key(x_api_key)
    success = await send_line_notify(body.line_user_id, body.message)
    return {"success": success, "line_user_id": body.line_user_id}


@app.post("/admin/test-sms")
async def test_sms(body: TestSMSRequest):
    """ทดสอบส่ง SMS ไปที่เบอร์ที่ระบุ"""
    success = await send_sms(body.phone, body.message)
    return {
        "success": success,
        "phone": body.phone,
        "message": body.message,
    }


@app.post("/admin/import")
async def import_excel(x_api_key: str = Header(default=""), file: UploadFile = File(...)):
    """รับไฟล์ Excel แล้ว import orders + shipping + shipments เข้า Supabase"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="รองรับเฉพาะไฟล์ .xlsx หรือ .xls เท่านั้น")

    content = await file.read()
    buf = io.BytesIO(content)

    # โหลด sheets ที่มีอยู่
    xl = pd.ExcelFile(buf)
    available = xl.sheet_names

    def read_sheet(name):
        if name in available:
            buf.seek(0)
            return pd.read_excel(buf, sheet_name=name)
        return None

    df_orders      = read_sheet("Orders")
    df_shipping    = read_sheet("Shipping")
    df_accounting  = read_sheet("Accounting")
    df_summary     = read_sheet("Daily Summary")

    if df_orders is None and df_shipping is None:
        raise HTTPException(status_code=400, detail=f"ไม่พบ sheet Orders หรือ Shipping ในไฟล์นี้")

    sb = get_supabase()
    stats = {"orders": 0, "shipping": 0, "tracking_added": 0, "accounting": 0, "daily_summary": 0, "points": 0, "tracking_list": []}

    # ---- Import Orders ----
    order_rows = []
    for _, r in df_orders.iterrows():
        order_id = safe_val(r.get("Order ID"))
        if not order_id:
            continue
        order_rows.append({
            "order_id":     str(order_id),
            "order_date":   safe_date(r.get("Order Date")),
            "ship_date":    safe_date(r.get("Ship Date")),
            "customer":     safe_val(r.get("Customer")),
            "phone":        (lambda p: p.zfill(10) if p.isdigit() and len(p) < 10 else p)(str(int(float(safe_val(r.get("Phone")))) if safe_val(r.get("Phone")) and str(safe_val(r.get("Phone"))).replace('.','').isdigit() else safe_val(r.get("Phone")) or "")),
            "province":     safe_val(r.get("Province")),
            "zip":          str(safe_val(r.get("ZIP")) or ""),
            "full_address": safe_val(r.get("Full Address")),
            "note":         safe_val(r.get("Note")),
            "sku":          safe_val(r.get("SKU")),
            "qty":          int(r["Qty"]) if pd.notna(r.get("Qty")) else None,
            "channel":      safe_val(r.get("Channel")),
            "status":       safe_val(r.get("Status")),
        })

    # deduplicate order_id ในกรณีซ้ำในไฟล์เดียวกัน
    seen_orders = {}
    for row in order_rows:
        if row["order_id"]:
            seen_orders[row["order_id"]] = row
    order_rows = list(seen_orders.values())
    for i in range(0, len(order_rows), 50):
        sb.table("orders").upsert(order_rows[i:i+50], on_conflict="order_id").execute()
    stats["orders"] = len(order_rows)

    # ---- point_ledger: สะสมแต้ม "เฉพาะออเดอร์ในระบบเว็บตัวเอง" เท่านั้น ----
    # ออเดอร์ Shopee (channel != web) ไม่ให้แต้ม ตามนโยบายร้าน
    point_rows = []
    skipped_no_date = 0
    skipped_unpaid  = 0
    for row in order_rows:
        if (row.get("channel") or "shopee") != "web":
            continue  # ข้าม Shopee — ไม่สะสมแต้ม
        if row.get("status") != "ชำระแล้ว":
            skipped_unpaid += 1
            continue  # ให้ point เฉพาะ order ที่ชำระเงินแล้วเท่านั้น
        phone = row.get("phone")
        if not phone:
            continue  # point_ledger ต้องมีเบอร์โทร ข้ามถ้าไม่มี
        order_date = row.get("order_date")
        if not order_date:
            # safe_date() อาจ return None ถ้าวันที่ใน Excel ผิดรูปแบบ/ว่าง
            # point_ledger.order_date เป็น NOT NULL — ข้ามแถวนี้ ไม่ให้พังทั้ง batch
            skipped_no_date += 1
            continue
        ml = parse_shopee_sku_ml(row.get("sku") or "")
        point_rows.append({
            "order_id":   row["order_id"],
            "phone":      phone,
            "customer":   row.get("customer"),
            "channel":    row.get("channel") or "shopee",
            "ml_total":   ml,
            "points":     ml / 100,
            "order_date": order_date,
        })
    if point_rows:
        for i in range(0, len(point_rows), 50):
            sb.table("point_ledger").upsert(point_rows[i:i+50], on_conflict="order_id").execute()
    stats["points"] = len(point_rows)
    if skipped_no_date:
        print(f"[point_ledger] ข้าม {skipped_no_date} order ที่ไม่มี order_date ที่ถูกต้อง")
    if skipped_unpaid:
        print(f"[point_ledger] ข้าม {skipped_unpaid} order ที่ยังไม่ชำระเงิน (จะได้ point ตอน import รอบหน้าถ้าสถานะอัปเดตเป็นชำระแล้ว)")

    # ---- Import Shipping ----
    shipping_rows = []
    tracking_to_add = []

    for _, r in df_shipping.iterrows():
        # ข้ามแถว TOTAL หรือแถวที่ไม่มี Order ID
        order_id_raw = str(r.get("Order ID") or "").strip()
        if not order_id_raw or order_id_raw.upper() == "TOTAL" or order_id_raw == "nan":
            continue
        carrier_raw = str(r.get("Carrier") or "").strip()
        cu = carrier_raw.upper()
        if "FLASH" in cu:
            carrier = "Flash Express"
        elif "POST" in cu or "SABUY" in cu:
            carrier = "POST SABUY"
        elif "KEX" in cu or "KERRY" in cu:
            carrier = "KEX Express"
        elif "OWN" in cu or "SELLER" in cu:
            carrier = "Seller Own Fleet"
        else:
            carrier = carrier_raw

        weight = r.get("Weight (g)") or r.get("Weight(g)")
        cost   = r.get("Shipping Cost (฿)") or r.get("Shipping Cost(฿)")
        tracking = safe_val(r.get("Tracking"))

        shipping_rows.append({
            "order_id":      str(safe_val(r.get("Order ID")) or ""),
            "ship_date":     safe_date(r.get("Ship Date")),
            "carrier":       carrier,
            "tracking":      str(tracking) if tracking else None,
            "weight_g":      int(float(str(weight).replace(",",""))) if pd.notna(weight) and str(weight).strip() not in ["", "-", "N/A"] else None,
            "shipping_cost": float(str(cost).replace(",","")) if pd.notna(cost) and str(cost).strip() not in ["", "-", "N/A"] else None,
        })

        # เก็บ tracking ที่ต้องให้ cron เช็ค — ทุกขนส่งที่มีเลขจริง (Flash TH / KEX SXF / POST JM)
        # ยกเว้น Seller Own Fleet (tracking = "-")
        if tracking and str(tracking).strip() and str(tracking).strip() != "-":
            tracking_to_add.append(str(tracking).strip().upper())

    # กรองเฉพาะ row ที่มี order_id จริงๆ
    shipping_rows = [r for r in shipping_rows if r.get("order_id") and r["order_id"].strip()]
    for i in range(0, len(shipping_rows), 50):
        sb.table("shipping").upsert(shipping_rows[i:i+50], on_conflict="tracking", ignore_duplicates=True).execute()
    stats["shipping"] = len(shipping_rows)

    # ---- เพิ่ม Tracking เข้า Shipments ----
    if tracking_to_add:
        shipment_rows = [{"barcode": t, "status": "pending", "is_done": False} for t in tracking_to_add]
        sb.table("shipments").upsert(shipment_rows, on_conflict="barcode", ignore_duplicates=True).execute()
        stats["tracking_added"] = len(tracking_to_add)
        stats["tracking_list"]  = tracking_to_add

    # ---- Import Accounting ----
    if df_accounting is not None:
        acc_rows = []
        for _, r in df_accounting.iterrows():
            order_id = safe_val(r.get("Order ID"))
            if not order_id:
                continue
            def safe_num(v):
                try:
                    return float(v) if pd.notna(v) else None
                except:
                    return None
            def get_col(r, *names):
                for n in names:
                    v = r.get(n)
                    if v is not None and not (isinstance(v, float) and pd.isna(v)):
                        return v
                return None
            acc_rows.append({
                "order_id":    str(order_id),
                "order_date":  safe_date(r.get("Order Date")),
                "customer":    safe_val(r.get("Customer")),
                "revenue":     safe_num(get_col(r, "Revenue (฿)", "Revenue(฿)", "Revenue")),
                "shopee_net":  safe_num(get_col(r, "Shopee Net (฿)", "Shopee Net(฿)", "Shopee Net")),
                "shopee_fee":  safe_num(get_col(r, "Shopee Fee (฿)", "Shopee Fee(฿)", "Shopee Fee", "Fee(฿)", "Fee")),
                "shipping":    safe_num(get_col(r, "Shipping (฿)", "Shipping(฿)", "Shipping")),
                "coffee_cost": safe_num(get_col(r, "Coffee Cost (฿)", "Coffee Cost(฿)", "Coffee Cost")),
                "packaging":   safe_num(get_col(r, "Packaging (฿)", "Packaging(฿)", "Packaging")),
                "other":       safe_num(r.get("Other")) or 0,
                "net_profit":  safe_num(get_col(r, "Net Profit (฿)", "Net Profit(฿)", "Net Profit")),
                "note":        safe_val(r.get("Note")),
            })
        if acc_rows:
            sb.table("accounting").upsert(acc_rows, on_conflict="order_id").execute()
            stats["accounting"] = len(acc_rows)

    # ---- Import Daily Summary ----
    if df_summary is not None:
        sum_rows = []
        for _, r in df_summary.iterrows():
            ship_date = safe_date(r.get("Ship Date"))
            raw_date = str(r.get("Ship Date") or "").strip()
            if not ship_date or raw_date.upper() == "TOTAL" or not raw_date:
                continue
            def safe_num(v):
                try:
                    return float(v) if pd.notna(v) else None
                except:
                    return None
            def get_col(r, *names):
                for n in names:
                    v = r.get(n)
                    if v is not None and not (isinstance(v, float) and pd.isna(v)):
                        return v
                return None
            sum_rows.append({
                "ship_date":     ship_date,
                "orders":        int(r["Orders"]) if pd.notna(r.get("Orders")) else None,
                "units":         int(r["Units"]) if pd.notna(r.get("Units")) else None,
                "revenue":       safe_num(get_col(r, "Revenue (฿)", "Revenue(฿)", "Revenue")),
                "shopee_net":    safe_num(get_col(r, "Shopee Net (฿)", "Shopee Net(฿)", "Shopee Net")),
                "fee":           safe_num(get_col(r, "Fee (฿)", "Fee(฿)", "Fee")),
                "shipping":      safe_num(get_col(r, "Shipping (฿)", "Shipping(฿)", "Shipping")),
                "coffee_cost":   safe_num(get_col(r, "Coffee Cost (฿)", "Coffee Cost(฿)", "Coffee Cost")),
                "packaging":     safe_num(get_col(r, "Packaging (฿)", "Packaging(฿)", "Packaging")),
                "net_profit":    safe_num(get_col(r, "Net Profit (฿)", "Net Profit(฿)", "Net Profit")),
                "margin_pct":    (lambda v: round(v * 100, 2) if v is not None and v < 1 else v)(safe_num(get_col(r, "Margin %", "Margin%"))),
                "avg_per_order": safe_num(get_col(r, "Avg/Order (฿)", "Avg/Order(฿)", "Avg/Order")),
            })
        if sum_rows:
            # deduplicate โดยเอา ship_date ล่าสุดในกรณีซ้ำ
            seen = {}
            for row in sum_rows:
                seen[row["ship_date"]] = row
            sum_rows = list(seen.values())
            sb.table("daily_summary").upsert(sum_rows, on_conflict="ship_date").execute()
            stats["daily_summary"] = len(sum_rows)

    return {
        "success": True,
        "filename": file.filename,
        "imported": stats,
        "message": f"Import สำเร็จ — {stats['orders']} orders, {stats['shipping']} shipping, {stats['tracking_added']} tracking, {stats['accounting']} accounting, {stats['daily_summary']} daily summary, {stats['points']} point records"
    }


# ---- Create Web Order ----
class OrderItem(BaseModel):
    sku: str
    qty: int
    price: float
    name: str

class CreateOrderRequest(BaseModel):
    order_id:             str
    customer:             str
    phone:                str
    full_address:         str
    province:             str
    zip:                  str
    note:                 Optional[str] = ""
    items:                list[OrderItem]
    total:                float
    channel:              str = "web"
    status:               str = "รอชำระเงิน"
    first_order_discount: bool = False  # ส่วนลด 50% ครั้งแรก (backend คำนวณจริงเอง)
    subtotal:             Optional[float] = None  # ยอดก่อนหักส่วนลด (อ้างอิง — backend คำนวณจาก items เอง)
    preferred_carrier:    Optional[str] = None  # ขนส่งที่ลูกค้าเลือก: "thailand_post" | "kex"
    line_user_id:         Optional[str] = None  # ถ้า login LINE — ใช้ส่งแจ้งเตือนรับออเดอร์ทาง LINE
    account_phone:        Optional[str] = None  # เบอร์บัญชีคนที่ login (LINE/OTP) — ใช้ผูกแต้มเข้าบัญชีคนสั่ง
    # UTM tracking — มาจากไหน (โฆษณา/แคมเปญ) ส่งมาจากหน้าเว็บตอน checkout
    utm_source:           Optional[str] = None
    utm_medium:           Optional[str] = None
    utm_campaign:         Optional[str] = None
    utm_content:          Optional[str] = None
    utm_term:             Optional[str] = None
    referrer:             Optional[str] = None
    landing_page:         Optional[str] = None

def sku_code_to_ml(sku_code: str) -> int:
    """แปลง SKU code (เช่น ORIGINAL-200, KYOHO, ORIGINAL) เป็น ml — ใช้กับ order จากเว็บเท่านั้น"""
    code = (sku_code or "").upper().strip()
    if code.endswith("-200"):
        return 200
    if code in ("KYOHO", "GESHA"):
        return 200
    return 1000  # Cold Brew ขนาดมาตรฐาน 1L


def parse_shopee_sku_ml(sku_str: str) -> int:
    """
    คำนวณ ml รวมจาก SKU string ของ order ที่ import จาก Excel (Shopee)
    รองรับรูปแบบอิสระ เช่น "Original 1L x1, Honey 200ml x1" / "Dark 1L" / "Original 1L / Fruity 1L"
    ตัด RTD ออกทั้งหมด (ไม่ได้ point)
    ทดสอบแล้วกับ SKU จริง 100 รายการจากระบบ
    """
    if not sku_str:
        return 0

    items = re.split(r'[,+/]', sku_str)
    total_ml = 0
    for item in items:
        item = item.strip()
        if not item:
            continue

        # ตัด RTD ออกทั้งหมด (ไม่สนตัวพิมพ์ใหญ่เล็ก, ไม่สนขีดล่าง)
        if re.search(r'rtd', item, re.IGNORECASE):
            continue

        # หาขนาดที่ระบุไว้ชัดเจน: 1L หรือ 200ml
        size_match = re.search(r'(\d+)\s*(ml|l)\b', item, re.IGNORECASE)
        if size_match:
            size_num  = int(size_match.group(1))
            size_unit = size_match.group(2).lower()
            ml = size_num * 1000 if size_unit == 'l' else size_num
        else:
            # ไม่มีขนาดระบุ -> Cold Drip/Gesha/Kyoho/ขนาดทดลอง default 200ml, อื่นๆ default 1L
            if re.search(r'cold\s*drip|gesha|kyoho|ขนาดทดลอง', item, re.IGNORECASE):
                ml = 200
            else:
                ml = 1000

        qty_match = re.search(r'x\s*(\d+)\b', item, re.IGNORECASE)
        qty = int(qty_match.group(1)) if qty_match else 1

        total_ml += ml * qty

    return total_ml


# ---- ต้นทุน/บัญชี ออเดอร์เว็บ ----
COFFEE_COST_1L   = {"ORIGINAL": 89.0, "DARK": 75.60, "HONEY": 97.0, "FRUITY": 109.0, "NUTTY": 122.40}
COFFEE_COST_DRIP = {"GESHA": 61.20, "KYOHO": 61.20}  # cold drip 200ml

def _coffee_cost_per_unit(sku: str) -> float:
    s = (sku or "").upper().strip()
    if s in COFFEE_COST_1L:
        return COFFEE_COST_1L[s]
    if s in COFFEE_COST_DRIP:
        return COFFEE_COST_DRIP[s]
    if s.endswith("-200"):  # ขนาดทดลอง 200ml = ต้นทุนเมล็ด 1L / 8
        return round(COFFEE_COST_1L.get(s.replace("-200", ""), 0.0) / 8, 2)
    return 0.0

def _web_order_costs(items):
    """คืน (coffee_cost, packaging) จาก items ของออเดอร์เว็บ ตามสูตรต้นทุน+แพ็กเกจของร้าน"""
    coffee = 0.0
    qty_1l = 0
    qty_200 = 0
    for it in items:
        sku = (it.sku or "").upper().strip()
        coffee += _coffee_cost_per_unit(sku) * it.qty
        if sku.endswith("-200") or sku in ("KYOHO", "GESHA"):
            qty_200 += it.qty
        else:
            qty_1l += it.qty
    # packaging 1L: 1ชิ้น=11.70 | 2+ชิ้น=(qty×4.5)+(qty×3.9)+2
    pack_1l = 0.0 if qty_1l == 0 else (11.70 if qty_1l == 1 else qty_1l * 4.5 + qty_1l * 3.9 + 2)
    # packaging 200ml: 1ชิ้น=6.15 | 2+ชิ้น=(qty×2)+3.9+2
    pack_200 = 0.0 if qty_200 == 0 else (6.15 if qty_200 == 1 else qty_200 * 2 + 3.9 + 2)
    return round(coffee, 2), round(pack_1l + pack_200, 2)


def _costs_from_sku_string(sku_str: str):
    """คำนวณ (coffee_cost, packaging) จาก sku string ของออเดอร์เก่า (รูปแบบ 'ชื่อสินค้า xN, ...')
    ใช้ตอน backfill บัญชีย้อนหลัง — จับ flavor จากชื่อ + ดูขนาดจาก 'ขนาดทดลอง'/Cold Drip"""
    coffee = 0.0
    q1 = 0
    q2 = 0
    for seg in (sku_str or "").split(","):
        seg = seg.strip()
        if not seg:
            continue
        m = re.search(r'x\s*(\d+)', seg, re.IGNORECASE)
        qty = int(m.group(1)) if m else 1
        low = seg.lower()
        flavor = next((f for f in ("original", "dark", "honey", "nutty", "fruity", "kyoho", "gesha") if f in low), None)
        if not flavor:
            continue
        flavor = flavor.upper()
        is_200 = ("ขนาดทดลอง" in seg) or flavor in ("KYOHO", "GESHA")
        if flavor in ("KYOHO", "GESHA"):
            cpu = COFFEE_COST_DRIP.get(flavor, 0.0)
        elif is_200:
            cpu = round(COFFEE_COST_1L.get(flavor, 0.0) / 8, 2)
        else:
            cpu = COFFEE_COST_1L.get(flavor, 0.0)
        coffee += cpu * qty
        if is_200:
            q2 += qty
        else:
            q1 += qty
    pack_1l = 0.0 if q1 == 0 else (11.70 if q1 == 1 else q1 * 4.5 + q1 * 3.9 + 2)
    pack_200 = 0.0 if q2 == 0 else (6.15 if q2 == 1 else q2 * 2 + 3.9 + 2)
    return round(coffee, 2), round(pack_1l + pack_200, 2)


@app.post("/orders/create")
async def create_order(body: CreateOrderRequest):
    """สร้าง order จากหน้าเว็บ — ยังไม่ให้ point ตรงนี้ ต้องรอยืนยันการชำระเงินก่อน (ดู /admin/confirm-payment)"""
    sb = get_supabase()

    # sku: ใช้ชื่อสินค้า (แสดงให้ลูกค้า/admin อ่านง่าย)
    sku_str = ", ".join([f"{i.name} x{i.qty}" for i in body.items])

    # ── คำนวณส่วนลดลูกค้าใหม่เป็น "ตัวจริง" ที่ฝั่ง server ──
    # ให้ส่วนลดเมื่อ: (1) หน้าเว็บขอมา (แสดงยอดลดให้ลูกค้าเห็นแล้ว) + (2) promo เปิด + (3) เบอร์มีสิทธิ์จริง
    # เงื่อนไข (1) กันไม่ให้ยอดที่ชาร์จต่างจากที่ลูกค้าเห็นบนหน้า checkout | (3) กันการแก้ค่าเพื่อขอสิทธิ์ซ้ำ
    subtotal   = sum(i.price * i.qty for i in body.items)
    # เบอร์บัญชีของคนสั่ง (login) — ใช้ตัดสินสิทธิ์ ไม่ใช่เบอร์ผู้รับ (กันเปลี่ยนเบอร์ผู้รับเพื่อขอสิทธิ์ซ้ำ)
    elig_phone = (body.account_phone or "").strip()
    if body.line_user_id:
        try:
            _c = sb.table("customers").select("phone").eq("line_user_id", body.line_user_id).execute()
            if _c.data and _c.data[0].get("phone"):
                elig_phone = _c.data[0]["phone"]
        except Exception:
            pass
    if not elig_phone:
        elig_phone = (body.phone or "").strip()
    has_account = bool(body.line_user_id or body.account_phone)  # ต้อง login เท่านั้น (guest ไม่ได้ส่วนลด)
    eligible   = (
        bool(body.first_order_discount)
        and has_account
        and _promo_first_order_enabled()
        and _is_first_order_eligible(sb, elig_phone)
    )
    discount   = _first_order_discount(subtotal) if eligible else 0
    total_paid = subtotal - discount   # ยอดที่ลูกค้าจ่ายจริง (Pixel Purchase ใช้ค่านี้)

    order_row = {
        "order_id":     body.order_id,
        "order_date":   datetime.utcnow().strftime("%Y-%m-%d"),
        "created_at":   datetime.utcnow().isoformat(),  # ตั้งเวลาสร้างชัดเจน เพื่อให้ cron ลบ order ค้างชำระได้ตรงเวลา
        "customer":     body.customer,
        "phone":        body.phone.zfill(10) if body.phone.isdigit() and len(body.phone) < 10 else body.phone,
        "full_address": body.full_address,
        "province":     body.province,
        "zip":          body.zip,
        "note":         body.note,
        "sku":          sku_str,
        "qty":          sum(i.qty for i in body.items),
        "channel":      body.channel,
        "status":       body.status,
        "total":        total_paid,
        "first_order_discount": discount > 0,
    }
    # เก็บยอดที่ลดจริง — ใส่เฉพาะเมื่อมีส่วนลด เพื่อไม่ให้ order ปกติพังถ้ายังไม่ได้ ALTER TABLE
    # (ต้องรัน migrations/2026-07-21_add_discount_amount.sql ก่อนเปิดโปร)
    if discount > 0:
        order_row["discount_amount"] = discount

    # ขนส่งที่ลูกค้าเลือก — ใส่เฉพาะเมื่อมีค่า (ต้องรัน migrations/2026-07-22_add_preferred_carrier.sql ก่อน)
    if body.preferred_carrier:
        order_row["preferred_carrier"] = body.preferred_carrier

    # บัญชีคนที่ login สั่ง — ใช้ผูกแต้มเข้าบัญชีคนสั่ง (ต้องรัน migrations/2026-07-22_add_line_user_id.sql ก่อน)
    if body.line_user_id:
        order_row["line_user_id"] = body.line_user_id
    # เบอร์บัญชีคนที่ login (LINE/OTP) — สำหรับผูกแต้ม (ต้องรัน migrations/2026-07-22_add_account_phone.sql ก่อน)
    if body.account_phone:
        order_row["account_phone"] = body.account_phone

    # UTM tracking — ใส่เฉพาะ field ที่มีค่า เพื่อไม่ให้ order ปกติพังถ้ายังไม่ได้ ALTER TABLE
    # (ต้องรัน SQL เพิ่มคอลัมน์ใน orders ก่อน ดู migrations/2026-07-21_add_utm.sql)
    utm_fields = {
        "utm_source":   body.utm_source,
        "utm_medium":   body.utm_medium,
        "utm_campaign": body.utm_campaign,
        "utm_content":  body.utm_content,
        "utm_term":     body.utm_term,
        "referrer":     body.referrer,
        "landing_page": body.landing_page,
    }
    for k, v in utm_fields.items():
        if v:
            order_row[k] = v

    sb.table("orders").insert(order_row).execute()

    # เก็บลูกค้าลงตาราง customers อัตโนมัติ — เฉพาะเบอร์ที่ยังไม่มี (ไม่ทับข้อมูลเดิมของคน login)
    try:
        cust_phone = (body.phone or "").replace("-", "").replace(" ", "").strip()
        if cust_phone:
            exist = sb.table("customers").select("phone").eq("phone", cust_phone).limit(1).execute()
            if not exist.data:
                sb.table("customers").insert({
                    "phone":        cust_phone,
                    "name":         body.customer,
                    "display_name": body.customer,
                    "address":      body.full_address,
                    "province":     body.province,
                    "zip":          body.zip,
                }).execute()
                print(f"[customer] auto-capture guest {body.customer} ({cust_phone})")
    except Exception as e:
        print(f"[customer] auto-capture error: {e}")

    # mark first_order_used ที่ "เบอร์บัญชี" (elig_phone) เพื่อกันใช้ส่วนลด 50% ซ้ำ
    if discount > 0:
        try:
            upd = sb.table("customers").update({"first_order_used": True}).eq("phone", elig_phone).execute()
            if not upd.data:
                sb.table("customers").insert({
                    "phone":            elig_phone,
                    "first_order_used": True,
                }).execute()
            print(f"[first_order] marked used for {elig_phone}")
        except Exception as e:
            print(f"[first_order] mark error: {e}")

    # บันทึกบัญชีออเดอร์เว็บ — รายรับ + ต้นทุน + กำไร (ค่าส่งอัปเดตตอน add-shipping)
    coffee_cost, packaging = _web_order_costs(body.items)
    sb.table("accounting").upsert({
        "order_id":   body.order_id,
        "order_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "customer":   body.customer,
        "revenue":    total_paid,
        "shopee_net": total_paid,   # เว็บไม่มี fee → net = revenue
        "shopee_fee": 0,
        "shipping":   0,            # เว็บส่งฟรีลูกค้า; ต้นทุนส่งจริงอัปเดตตอนใส่เลขพัสดุ
        "coffee_cost": coffee_cost,
        "packaging":  packaging,
        "other":      0,
        "net_profit": round(total_paid - coffee_cost - packaging, 2),
        "note":       "web",
    }, on_conflict="order_id").execute()

    # แจ้ง admin ตอนมีออเดอร์ใหม่จากเว็บ
    sku_summary = ", ".join([f"{i.name} x{i.qty}" for i in body.items])
    await send_line_notify(
        ADMIN_LINE_USER_ID,
        f"🛒 ออเดอร์ใหม่! {body.customer} ({body.phone})\n"
        f"สินค้า: {sku_summary}\n"
        f"ยอด: ฿{total_paid:,.0f}" + (f" (ลดลูกค้าใหม่ -฿{discount:,.0f})" if discount > 0 else "") + "\n"
        f"Order ID: {body.order_id}"
    )

    # แจ้งลูกค้าทาง LINE ว่ารับออเดอร์แล้ว (ถ้า login LINE และไม่ได้ปิดแจ้งเตือน)
    try:
        line_uid  = body.line_user_id or ""
        notify_ch = None
        if line_uid:
            look = sb.table("customers").select("notify_channel").eq("line_user_id", line_uid).execute()
            if look.data:
                notify_ch = look.data[0].get("notify_channel")
        elif body.phone:
            look = sb.table("customers").select("line_user_id,notify_channel").eq("phone", body.phone).execute()
            if look.data:
                line_uid  = look.data[0].get("line_user_id") or ""
                notify_ch = look.data[0].get("notify_channel")
        channel = notify_ch or ("line" if line_uid else "sms")
        if channel == "line" and line_uid:
            cust_msg = (
                f"VeLA Cold Brew: รับออเดอร์ #{body.order_id} แล้วค่ะ 🐰\n"
                f"ยอดชำระ ฿{total_paid:,.0f}\n"
                f"กรุณาโอนเงินแล้วแนบสลิปเพื่อยืนยัน เดี๋ยวจัดส่งให้ไวๆ เลยค่ะ\n"
                f"ดูออเดอร์ / แนบสลิป: velacoldbrew.com/account"
            )
            await send_line_notify(line_uid, cust_msg, barcode=body.order_id, status="order_created",
                                   customer=body.customer, phone=body.phone)
    except Exception as e:
        print(f"[order-notify] LINE error: {e}")

    return {"success": True, "order_id": body.order_id, "total": total_paid, "discount": discount}


class SlipNotifyRequest(BaseModel):
    order_id: str
    slip_url: str

SLIPOK_API_URL  = os.getenv("SLIPOK_URL", "https://api.slipok.com/api/line/apikey/70860")
SLIPOK_API_KEY  = os.getenv("SLIPOK_API_KEY", "")
PROMPTPAY_ID    = os.getenv("PROMPTPAY_ID", "")


@app.post("/orders/slip-notify")
async def slip_notify(body: SlipNotifyRequest):
    """ตรวจสอบสลิปผ่าน SlipOK → auto-confirm ถ้ายอดตรง หรือแจ้ง admin"""
    sb = get_supabase()
    res = sb.table("orders").select("customer,phone,total,status,first_order_discount").eq("order_id", body.order_id).execute()
    order = res.data[0] if res.data else {}
    customer = order.get("customer", "ลูกค้า")
    phone    = order.get("phone", "")
    total    = float(order.get("total") or 0)

    if order.get("status") in ("ชำระแล้ว", "จัดส่งแล้ว", "จัดส่งสำเร็จ"):
        return {"success": True, "verified": False, "reason": "already_paid"}

    slip_verified = False
    slip_amount   = None
    slip_ref      = None
    slip_error    = None

    if SLIPOK_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    SLIPOK_API_URL,
                    headers={"x-authorization": SLIPOK_API_KEY},
                    data={"url": body.slip_url, "log": "true", "amount": str(int(total)) if total > 0 else ""},
                )
                rdata = r.json()
                if rdata.get("success"):
                    slip_amount   = float(rdata.get("data", {}).get("amount") or 0)
                    slip_ref      = rdata.get("data", {}).get("transRef", "")
                    slip_verified = True
                else:
                    err_code = rdata.get("code", 0)
                    if err_code == 1013:   slip_error = "ยอดในสลิปไม่ตรงกับยอด order"
                    elif err_code == 1014: slip_error = "สลิปนี้ไม่ได้โอนเข้าบัญชีร้าน"
                    elif err_code == 1010: slip_error = "สลิปซ้ำ เคยใช้ไปแล้ว"
                    else:                  slip_error = rdata.get("message", "ตรวจสอบสลิปไม่สำเร็จ")
        except Exception as e:
            slip_error = f"SlipOK error: {e}"
            print(f"[SlipOK] error: {e}")

    if slip_verified:
        sb.table("orders").update({
            "status":      "ชำระแล้ว",
            "paid_at":     datetime.utcnow().isoformat(),
            "slip_url":    body.slip_url,
            "slip_status": "verified",
        }).eq("order_id", body.order_id).execute()
        # ให้ points + mark first_order_used
        try:
            await _award_points(sb, body.order_id)
            await _mark_first_order_used(sb, body.order_id)
        except Exception as e:
            print(f"[SlipOK] post-confirm error: {e}")
        await send_line_notify(ADMIN_LINE_USER_ID,
            f"✅ Auto-confirm! {customer} ({phone})\nOrder: {body.order_id}\nยอด: ฿{slip_amount:,.0f}\nRef: {slip_ref}")
        # แจ้งลูกค้าทาง LINE ว่ายืนยันการชำระเงินแล้ว
        await _notify_customer_line(sb, body.order_id, phone, customer,
            f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{body.order_id} เรียบร้อยแล้วค่ะ ✅\n"
            f"กำลังแพ็คของและจัดส่งให้เร็วที่สุด เดี๋ยวมีเลขพัสดุแจ้งอีกทีนะคะ 🐰\n"
            f"ดูสถานะ: velacoldbrew.com/account",
            "payment_confirmed")
        return {"success": True, "verified": True, "amount": slip_amount, "ref": slip_ref}
    else:
        sb.table("orders").update({
            "slip_url":    body.slip_url,
            "slip_status": "rejected" if slip_error else "pending",
        }).eq("order_id", body.order_id).execute()
        msg = (f"💳 ลูกค้าส่งสลิปแล้ว!\nชื่อ: {customer}" +
               (f" ({phone})" if phone else "") +
               f"\nOrder: {body.order_id}" +
               (f"\nยอด order: ฿{total:,.0f}" if total else "") +
               (f"\n⚠️ {slip_error}" if slip_error else "") +
               f"\nกรุณายืนยันชำระเงินใน /admin/orders")
        await send_line_notify(ADMIN_LINE_USER_ID, msg)
        return {"success": True, "verified": False, "reason": slip_error or "manual_check"}


@app.get("/orders/qr/{order_id}")
async def get_order_qr(order_id: str):
    """สร้าง PromptPay QR Code เฉพาะ order นี้ พร้อม ref1=order_id"""
    if not PROMPTPAY_ID:
        raise HTTPException(status_code=500, detail="PROMPTPAY_ID not configured")

    sb = get_supabase()
    res = sb.table("orders").select("total,status").eq("order_id", order_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="order not found")
    order = res.data[0]
    total = float(order.get("total") or 0)

    try:
        from promptpay import qrcode as pp_qrcode
        import qrcode as qr_lib
        import io, base64

        # สร้าง PromptPay payload พร้อม amount และ ref1=order_id
        payload = pp_qrcode.generate_payload(
            PROMPTPAY_ID,
            {"amount": total, "ref1": order_id[:20]}  # ref1 max 20 chars
        )

        # สร้าง QR image
        img = qr_lib.make(payload, error_correction=qr_lib.constants.ERROR_CORRECT_M)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()

        return {
            "order_id": order_id,
            "amount":   total,
            "payload":  payload,
            "qr_base64": f"data:image/png;base64,{b64}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"QR generation error: {e}")




async def _mark_first_order_used(sb, order_id: str):
    """mark first_order_used = true ถ้า order นี้ใช้ส่วนลด 50% ครั้งแรก"""
    try:
        res = sb.table("orders").select("phone,first_order_discount").eq("order_id", order_id).execute()
        if not res.data:
            return
        order = res.data[0]
        if order.get("first_order_discount"):
            phone = order.get("phone")
            if phone:
                sb.table("customers").upsert({
                    "phone":            phone,
                    "first_order_used": True,
                }, on_conflict="phone").execute()
                print(f"[first_order] marked used for {phone}")
    except Exception as e:
        print(f"[first_order] mark error: {e}")

def _loyalty_identity(sb, order: dict):
    """คืน (phone, customer_name) สำหรับผูกแต้ม — ผูกกับ 'บัญชีคนที่ login สั่ง' เสมอ
    ลำดับความสำคัญ: LINE (line_user_id → เบอร์บัญชี) > เบอร์บัญชีที่ login (account_phone, ครอบคลุม OTP) > เบอร์ในออเดอร์ (guest)
    เพื่อกันเคสส่งหลายที่อยู่/เบอร์ผู้รับต่างกัน แล้วแต้มกระจาย"""
    # เบอร์บัญชีที่ login (LINE/OTP) มาก่อนเบอร์ผู้รับในออเดอร์
    phone = order.get("account_phone") or order.get("phone") or ""
    name  = order.get("customer") or ""
    luid  = order.get("line_user_id") or ""
    if luid:
        try:
            c = sb.table("customers").select("phone,name,display_name").eq("line_user_id", luid).execute()
            if c.data:
                acc_phone = c.data[0].get("phone")
                if acc_phone:
                    phone = acc_phone  # ใช้เบอร์บัญชีของคนสั่ง ไม่ใช่เบอร์ผู้รับ
                name = c.data[0].get("name") or c.data[0].get("display_name") or name
        except Exception as e:
            print(f"[loyalty] identity error: {e}")
    return phone, name


async def _award_points(sb, order_id: str):
    """ให้ point จาก order — แยกออกมาเพื่อให้เรียกซ้ำได้"""
    res = sb.table("orders").select("*").eq("order_id", order_id).execute()
    if not res.data:
        return
    order = res.data[0]
    if (order.get("channel") or "web") != "web":
        return  # สะสมแต้มเฉพาะออเดอร์ในระบบเว็บตัวเอง
    # reuse logic เดิมจาก confirm_payment
    from_sku = order.get("sku", "")
    phone, customer = _loyalty_identity(sb, order)  # ผูกแต้มเข้าบัญชีคนสั่ง (login)
    # คำนวณ point จาก SKU (100ml = 1 point)
    total_ml = 0
    for item in (from_sku or "").split(","):
        item = item.strip()
        if "1000" in item or "1L" in item.upper():
            try:
                qty = int(item.split("x")[-1].strip()) if "x" in item else 1
                total_ml += 1000 * qty
            except:
                total_ml += 1000
    points = total_ml / 100
    if points > 0 and phone:
        try:
            sb.table("point_ledger").insert({
                "order_id":   order_id,
                "phone":      phone,
                "customer":   customer,
                "channel":    order.get("channel", "web"),
                "ml_total":   total_ml,
                "points":     points,
                "order_date": order.get("order_date", datetime.utcnow().strftime("%Y-%m-%d")),
            }).execute()
        except Exception as e:
            print(f"[point_ledger] {order_id}: {e}")



async def confirm_payment(order_id: str, x_api_key: str = Header(default="")):
    """ยืนยันการชำระเงิน — อัปเดตสถานะเป็น 'ชำระแล้ว' และให้ point ทันที"""
    check_admin_key(x_api_key)
    sb = get_supabase()

    res = sb.table("orders").select("*").eq("order_id", order_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail=f"ไม่พบ order_id: {order_id}")
    order = res.data[0]

    paid_at = datetime.utcnow().isoformat()

    sb.table("orders").update({
        "status":  "ชำระแล้ว",
        "paid_at": paid_at,
    }).eq("order_id", order_id).execute()

    phone = order.get("phone")
    point_result = None
    # สะสมแต้มเฉพาะออเดอร์ในระบบเว็บตัวเอง (ไม่รวม Shopee)
    if phone and (order.get("channel") or "web") == "web":
        pt_phone, pt_customer = _loyalty_identity(sb, order)  # ผูกแต้มเข้าบัญชีคนสั่ง (login)
        ml = parse_shopee_sku_ml(order.get("sku") or "")
        order_date = order.get("order_date") or datetime.utcnow().strftime("%Y-%m-%d")
        try:
            sb.table("point_ledger").upsert({
                "order_id":   order_id,
                "phone":      pt_phone,
                "customer":   pt_customer,
                "channel":    order.get("channel") or "web",
                "ml_total":   ml,
                "points":     ml / 100,
                "order_date": order_date,
            }, on_conflict="order_id").execute()
            point_result = {"ml_total": ml, "points": ml / 100}
        except Exception as e:
            print(f"[point_ledger] insert error (confirm-payment {order_id}): {e}")

    return {
        "success":  True,
        "order_id": order_id,
        "status":   "ชำระแล้ว",
        "paid_at":  paid_at,
        "points":   point_result,
    }


class AddShippingRequest(BaseModel):
    order_id:      str
    tracking:      str
    carrier:       str = "POST SABUY"
    ship_date:     Optional[str] = None
    weight_g:      Optional[int] = None
    shipping_cost: Optional[float] = None


@app.post("/admin/add-shipping")
async def add_shipping(body: AddShippingRequest, x_api_key: str = Header(default="")):
    """เพิ่มข้อมูลการจัดส่งจากหน้า admin — บันทึกลง shipping และ shipments"""
    check_admin_key(x_api_key)
    sb = get_supabase()

    ship_date = body.ship_date or datetime.utcnow().strftime("%Y-%m-%d")
    trk = body.tracking.strip().upper()
    # ชื่อขนส่งจริง — อิงจากเลข tracking เป็นหลัก (TH=Flash, SXF=KEX, JM=POST SABUY) แล้วค่อย fallback ที่ admin เลือก
    carrier = business_carrier(trk, body.carrier)

    # บันทึกลง shipping table
    sb.table("shipping").upsert({
        "order_id":      body.order_id,
        "ship_date":     ship_date,
        "carrier":       carrier,
        "tracking":      trk,
        "weight_g":      body.weight_g,
        "shipping_cost": body.shipping_cost,
    }, on_conflict="tracking").execute()

    # อัปเดตสถานะ order เป็นจัดส่งแล้ว
    sb.table("orders").update({
        "status":    "จัดส่งแล้ว",
        "ship_date": ship_date,
    }).eq("order_id", body.order_id).execute()

    # อัปเดตค่าส่งจริง + กำไรสุทธิ ในบัญชี (เฉพาะออเดอร์เว็บที่มีแถวบัญชีอยู่แล้ว)
    if body.shipping_cost is not None:
        try:
            acc = sb.table("accounting").select("revenue,coffee_cost,packaging,other") \
                .eq("order_id", body.order_id).execute()
            if acc.data:
                a = acc.data[0]
                ship = float(body.shipping_cost or 0)
                net = round(float(a.get("revenue") or 0) - float(a.get("coffee_cost") or 0)
                            - float(a.get("packaging") or 0) - ship - float(a.get("other") or 0), 2)
                sb.table("accounting").update({"shipping": ship, "net_profit": net}) \
                    .eq("order_id", body.order_id).execute()
        except Exception as e:
            print(f"[add-shipping] accounting update error: {e}")

    # เพิ่มลง shipments (tracking system) ถ้ายังไม่มี
    existing = sb.table("shipments").select("barcode").eq("barcode", body.tracking.strip().upper()).execute()
    if not existing.data:
        sb.table("shipments").insert({
            "barcode":  body.tracking.strip().upper(),
            "status":   "pending",
        }).execute()

    # แจ้งลูกค้าทาง LINE ว่าจัดส่งแล้ว (พร้อมเลขพัสดุ + ลิงก์ติดตาม)
    try:
        o = sb.table("orders").select("customer,phone").eq("order_id", body.order_id).execute()
        if o.data:
            if trk and trk != "-":
                ship_msg = (f"VeLA Cold Brew: ออเดอร์ #{body.order_id} จัดส่งแล้วค่ะ 🚚\n"
                            f"ขนส่ง: {carrier} · เลขพัสดุ: {trk}\n"
                            f"ติดตามพัสดุ: velacoldbrew.com/track/{trk}")
            else:
                ship_msg = (f"VeLA Cold Brew: ออเดอร์ #{body.order_id} จัดส่งแล้วค่ะ 🚚\n"
                            f"ขนส่ง: {carrier}\nขอบคุณที่สั่งซื้อนะคะ 🐰")
            await _notify_customer_line(sb, body.order_id, o.data[0].get("phone") or "", o.data[0].get("customer") or "",
                                        ship_msg, "shipped")
    except Exception as e:
        print(f"[add-shipping] notify error: {e}")

    return {"success": True, "order_id": body.order_id, "tracking": body.tracking.strip().upper()}


@app.post("/admin/sync-order-status")
async def sync_order_status(x_api_key: str = Header(default="")):
    """เชื่อมสถานะ orders ให้ตรงกับ shipments — พัสดุที่ส่งถึง/ตีกลับแล้ว → อัปเดต order ให้ตรง
    ใช้ sync ข้อมูลเก่าที่ค้าง 'จัดส่งแล้ว' ทั้งที่พัสดุถึงแล้ว (รันครั้งเดียวก็ได้)"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    fixed = []
    done = sb.table("shipments").select("barcode,status").eq("is_done", True).execute()
    for s in (done.data or []):
        target = "ตีกลับ" if s.get("status") == "returned" else "จัดส่งสำเร็จ"
        sr = sb.table("shipping").select("order_id").eq("tracking", s["barcode"]).execute()
        if not sr.data:
            continue
        oid = sr.data[0]["order_id"]
        cur = sb.table("orders").select("status").eq("order_id", oid).execute()
        if cur.data and cur.data[0].get("status") not in (target,):
            sb.table("orders").update({"status": target}).eq("order_id", oid).execute()
            fixed.append({"order_id": oid, "from": cur.data[0].get("status"), "to": target})
    return {"success": True, "count": len(fixed), "fixed": fixed}


@app.post("/admin/backfill-web-accounting")
async def backfill_web_accounting(x_api_key: str = Header(default="")):
    """คำนวณต้นทุน/กำไรย้อนหลังให้ออเดอร์เว็บที่ยังไม่มี (เช่น ออเดอร์ก่อนมีระบบบัญชี)
    เฉพาะแถวที่ coffee_cost ยังว่าง/เป็น 0 — ไม่ทับของที่คำนวณไว้แล้ว"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    # ค่าส่งจริงจาก shipping
    ship_map = {}
    for s in (sb.table("shipping").select("order_id,shipping_cost").execute().data or []):
        if s.get("order_id"):
            ship_map[s["order_id"]] = s.get("shipping_cost")
    # ออเดอร์เว็บ (WEB*)
    orders = sb.table("orders").select("order_id,order_date,customer,sku,total").like("order_id", "WEB%").execute()
    # accounting เดิม (ดู revenue + coffee_cost)
    acc_map = {}
    for a in (sb.table("accounting").select("order_id,revenue,coffee_cost").like("order_id", "WEB%").execute().data or []):
        acc_map[a["order_id"]] = a
    rows = []
    for o in (orders.data or []):
        oid = o["order_id"]
        existing = acc_map.get(oid) or {}
        # ข้ามถ้าคำนวณต้นทุนไว้แล้ว (coffee_cost > 0)
        if float(existing.get("coffee_cost") or 0) > 0:
            continue
        revenue = existing.get("revenue")
        if revenue is None:
            revenue = o.get("total") or 0
        coffee, packaging = _costs_from_sku_string(o.get("sku") or "")
        shipping = float(ship_map.get(oid) or 0)
        net = round(float(revenue or 0) - coffee - packaging - shipping, 2)
        rows.append({
            "order_id":   oid,
            "order_date": o.get("order_date"),
            "customer":   o.get("customer"),
            "revenue":    revenue,
            "shopee_net": revenue,
            "shopee_fee": 0,
            "shipping":   shipping,
            "coffee_cost": coffee,
            "packaging":  packaging,
            "other":      0,
            "net_profit": net,
            "note":       "web",
        })
    for i in range(0, len(rows), 50):
        sb.table("accounting").upsert(rows[i:i+50], on_conflict="order_id").execute()
    return {"success": True, "count": len(rows)}


@app.get("/admin/web-accounting")
async def web_accounting(x_api_key: str = Header(default="")):
    """ข้อมูลบัญชีออเดอร์เว็บ (อ่านผ่าน service key — ไม่เปิดตารางการเงินให้ anon)"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    acc = sb.table("accounting") \
        .select("order_id,order_date,customer,revenue,shopee_fee,shipping,coffee_cost,packaging,other,net_profit") \
        .like("order_id", "WEB%").order("order_date", desc=True).limit(3000).execute()
    st = {}
    for o in (sb.table("orders").select("order_id,status").like("order_id", "WEB%").limit(3000).execute().data or []):
        st[o["order_id"]] = o.get("status")
    rows = acc.data or []
    for r in rows:
        r["status"] = st.get(r["order_id"], "")
    return {"rows": rows}


@app.post("/admin/confirm-delivered")
async def confirm_delivered(order_id: str, notify: bool = True, x_api_key: str = Header(default="")):
    """ยืนยันส่งถึงแล้ว (กรณีส่งเอง) — อัปเดตสถานะและแจ้งลูกค้าผ่าน SMS/LINE"""
    check_admin_key(x_api_key)
    sb = get_supabase()

    res = sb.table("orders").select("*").eq("order_id", order_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail=f"ไม่พบ order_id: {order_id}")
    order = res.data[0]

    sb.table("orders").update({"status": "จัดส่งสำเร็จ"}).eq("order_id", order_id).execute()

    phone    = order.get("phone", "")
    customer = order.get("customer", "")

    ship_res = sb.table("shipping").select("tracking").eq("order_id", order_id).execute()
    tracking = ship_res.data[0].get("tracking", "") if ship_res.data else ""

    # mark shipments.is_done = true ถ้ามีเลข tracking อยู่ใน shipments table
    if tracking:
        try:
            sb.table("shipments").update({
                "is_done":         True,
                "status":          "delivered",
                "status_th":       "จัดส่งสำเร็จ",
                "last_checked_at": datetime.utcnow().isoformat(),
            }).eq("barcode", tracking.upper()).execute()
            print(f"[confirm-delivered] mark shipments done: {tracking}")
        except Exception as e:
            print(f"[confirm-delivered] shipments update error: {e}")

    if phone and notify:
        notify_ch = "sms"
        line_uid  = ""
        try:
            cust = sb.table("customers").select("notify_channel,line_user_id").eq("phone", phone).execute()
            if cust.data:
                line_uid  = cust.data[0].get("line_user_id") or ""
                # ลูกค้าที่ผูก LINE ไว้ → default รับแจ้งเตือนทาง LINE
                notify_ch = cust.data[0].get("notify_channel") or ("line" if line_uid else "sms")
        except:
            pass

        msg = "VeLA Cold Brew: พัสดุของคุณถึงแล้ว ✓ ขอบคุณที่สั่งซื้อนะคะ 🐰 สั่งซื้อและรับสิทธิพิเศษสมาชิกได้ที่: velacoldbrew.com"
        if notify_ch == "line" and line_uid:
            # ส่ง barcode/status/customer/phone ไปด้วย เพื่อให้ send_line_notify เขียน log ลง sms_logs (เหมือนฝั่ง SMS)
            await send_line_notify(line_uid, msg, barcode=tracking, status="delivered", customer=customer, phone=phone)
        else:
            await send_sms(phone, msg, barcode=tracking, status="delivered", customer=customer)

    return {"success": True, "order_id": order_id, "notified": bool(phone and notify)}


@app.post("/admin/confirm-payment")
async def confirm_payment(order_id: str, x_api_key: str = Header(default="")):
    """
    ยืนยันการชำระเงินของ order — แทนที่การ PATCH ตรงไปยัง Supabase จากหน้า admin เดิม
    ทำ 2 อย่างพร้อมกัน: (1) อัปเดต status เป็น 'ชำระแล้ว' พร้อม paid_at
                        (2) คำนวณ point จาก sku ของ order แล้วบันทึกลง point_ledger
    Point จะเข้าระบบ "ตอนยืนยันชำระเงินแล้วเท่านั้น" ไม่ใช่ตอนสั่งซื้อ
    """
    check_admin_key(x_api_key)
    sb = get_supabase()

    # ดึง order มาก่อน เพื่อเอา sku/phone/customer/channel/order_date ไปคำนวณ point
    res = sb.table("orders").select("*").eq("order_id", order_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail=f"ไม่พบ order_id: {order_id}")
    order = res.data[0]

    paid_at = datetime.utcnow().isoformat()

    # 1) อัปเดตสถานะเป็นชำระแล้ว
    sb.table("orders").update({
        "status":  "ชำระแล้ว",
        "paid_at": paid_at,
    }).eq("order_id", order_id).execute()

    # 2) คำนวณ point จาก sku string ที่เก็บไว้ — เฉพาะออเดอร์ในระบบเว็บตัวเอง (ไม่รวม Shopee)
    phone = order.get("phone")
    point_result = None
    if phone and (order.get("channel") or "web") == "web":
        pt_phone, pt_customer = _loyalty_identity(sb, order)  # ผูกแต้มเข้าบัญชีคนสั่ง (login)
        ml = parse_shopee_sku_ml(order.get("sku") or "")
        order_date = order.get("order_date") or datetime.utcnow().strftime("%Y-%m-%d")
        try:
            sb.table("point_ledger").upsert({
                "order_id":   order_id,
                "phone":      pt_phone,
                "customer":   pt_customer,
                "channel":    order.get("channel") or "web",
                "ml_total":   ml,
                "points":     ml / 100,
                "order_date": order_date,
            }, on_conflict="order_id").execute()
            point_result = {"ml_total": ml, "points": ml / 100}
        except Exception as e:
            print(f"[point_ledger] insert error (confirm-payment {order_id}): {e}")
    else:
        print(f"[point_ledger] ข้าม {order_id} — ไม่มีเบอร์โทร")

    # mark first_order_used ถ้า order นี้ใช้ส่วนลด 50%
    try:
        await _mark_first_order_used(sb, order_id)
    except Exception as e:
        print(f"[first_order] mark error: {e}")

    # แจ้งลูกค้าทาง LINE ว่ายืนยันการชำระเงินแล้ว
    await _notify_customer_line(sb, order_id, phone or "", order.get("customer") or "",
        f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{order_id} เรียบร้อยแล้วค่ะ ✅\n"
        f"กำลังแพ็คของและจัดส่งให้เร็วที่สุด เดี๋ยวมีเลขพัสดุแจ้งอีกทีนะคะ 🐰\n"
        f"ดูสถานะ: velacoldbrew.com/account",
        "payment_confirmed")

    return {
        "success":  True,
        "order_id": order_id,
        "status":   "ชำระแล้ว",
        "paid_at":  paid_at,
        "points":   point_result,
    }




# ---- OTP Login ----
import random
OTP_STORE: dict[str, dict] = {}  # phone -> {otp, expires}

class OTPRequestBody(BaseModel):
    phone: str

class OTPVerifyBody(BaseModel):
    phone: str
    otp:   str
    name:  Optional[str] = None

@app.post("/auth/request-otp")
async def request_otp(body: OTPRequestBody):
    """ส่ง OTP ไปยังเบอร์โทร"""
    phone = body.phone.replace("-","").replace(" ","").strip()
    if len(phone) < 9:
        raise HTTPException(status_code=400, detail="เบอร์โทรไม่ถูกต้อง")

    # สร้าง OTP 6 หลัก
    otp = str(random.randint(100000, 999999))
    import time
    OTP_STORE[phone] = {"otp": otp, "expires": time.time() + 300}  # หมดอายุ 5 นาที

    # ส่ง SMS
    msg = f"VeLA Cold Brew: รหัส OTP ของคุณคือ {otp} (หมดอายุใน 5 นาที)"
    success = await send_sms(phone, msg)

    if not success:
        raise HTTPException(status_code=500, detail="ส่ง OTP ไม่สำเร็จ")

    return {"success": True, "message": "ส่ง OTP แล้ว"}


@app.post("/auth/verify-otp")
async def verify_otp(body: OTPVerifyBody):
    """ตรวจสอบ OTP และ login/สร้าง account"""
    import time
    phone = body.phone.replace("-","").replace(" ","").strip()

    stored = OTP_STORE.get(phone)
    if not stored:
        raise HTTPException(status_code=400, detail="ไม่พบ OTP กรุณาขอใหม่")
    if time.time() > stored["expires"]:
        del OTP_STORE[phone]
        raise HTTPException(status_code=400, detail="OTP หมดอายุแล้ว กรุณาขอใหม่")
    if stored["otp"] != body.otp.strip():
        raise HTTPException(status_code=400, detail="OTP ไม่ถูกต้อง")

    # OTP ถูกต้อง — ลบทิ้ง
    del OTP_STORE[phone]

    sb = get_supabase()

    # เช็คว่ามี customer เบอร์นี้ไหม
    res = sb.table("customers").select("*").eq("phone", phone).execute()
    customer = res.data[0] if res.data else None

    if not customer:
        # สร้างใหม่
        name = body.name or f"ลูกค้า VeLA"
        ins = sb.table("customers").insert({
            "phone":        phone,
            "display_name": name,
        }).execute()
        customer = ins.data[0] if ins.data else {"phone": phone, "display_name": name}

        # แจ้ง admin ตอนมีสมาชิกใหม่สมัครด้วยเบอร์โทร
        await send_line_notify(
            ADMIN_LINE_USER_ID,
            f"🆕 สมาชิกใหม่! {name} ({phone}) สมัครผ่านเว็บ velacoldbrew.com"
        )

    return {"success": True, "customer": customer}

@app.get("/leaderboard")
async def get_leaderboard(limit: int = 10, phone: Optional[str] = None):
    """
    จัดอันดับลูกค้าตามยอด point ของเดือนปัจจุบัน (รวม Shopee + เว็บ ตามเบอร์โทรเดียวกัน)
    Point มาจาก ml รวมที่ดื่ม ไม่ใช่ยอดเงิน — 100ml = 1 point

    - limit: จำนวน top ranking ที่จะแสดง (ค่าเริ่มต้น 10)
    - phone: ถ้าระบุ จะคืนอันดับ/point ของเบอร์นี้มาด้วย แม้จะอยู่นอก top N ก็ตาม
    """
    sb = get_supabase()

    today = datetime.utcnow()
    month_start = today.strftime("%Y-%m-01")

    rows = sb.table("point_ledger") \
        .select("phone,customer,points,order_date") \
        .eq("channel", "web") \
        .gte("order_date", month_start) \
        .execute()

    data = rows.data or []

    # รวม point ตามเบอร์โทร (group by ทำใน Python เพราะข้อมูลไม่เยอะ)
    totals: dict[str, dict] = {}
    for r in data:
        p = r.get("phone")
        if not p:
            continue
        if p not in totals:
            totals[p] = {"phone": p, "customer": r.get("customer") or "", "points": 0.0}
        totals[p]["points"] += float(r.get("points") or 0)
        if r.get("customer"):
            totals[p]["customer"] = r["customer"]

    # ดึงชื่อที่แสดงจาก customers table (customers.name ที่ลูกค้าตั้งเอง หรือ display_name จาก LINE)
    phones = list(totals.keys())
    if phones:
        try:
            cust_res = sb.table("customers").select("phone,name,display_name") \
                .in_("phone", phones).execute()
            for c in (cust_res.data or []):
                p = c.get("phone")
                if p and p in totals:
                    # ใช้ customers.name ถ้ามี (ลูกค้าตั้งเอง) ไม่งั้นใช้ display_name (LINE)
                    display = c.get("name") or c.get("display_name") or totals[p]["customer"]
                    if display:
                        totals[p]["customer"] = display
        except Exception:
            pass

    # จัดอันดับทั้งหมดก่อน (ไม่ตัด limit) เพื่อให้หา rank ของเบอร์เฉพาะได้แม้อยู่นอก top N
    full_ranked = sorted(totals.values(), key=lambda x: x["points"], reverse=True)

    # ปิดบังเบอร์โทรบางส่วนเพื่อความเป็นส่วนตัว เช่น 091-XXX-456
    def mask_phone(p: str) -> str:
        p = p or ""
        if len(p) < 7:
            return p
        return f"{p[:3]}-XXX-{p[-3:]}"

    top_n = [
        {
            "rank":          i + 1,
            "customer":      r["customer"],
            "phone_masked":  mask_phone(r["phone"]),
            "points":        round(r["points"], 1),
        }
        for i, r in enumerate(full_ranked[:limit])
    ]

    response = {
        "month":   today.strftime("%Y-%m"),
        "results": top_n,
        "total_participants": len(full_ranked),
    }

    # ถ้าระบุเบอร์ — หา rank จริงของเบอร์นั้นในทั้งหมด (ไม่จำกัดแค่ top N)
    if phone:
        my_rank = None
        for i, r in enumerate(full_ranked):
            if r["phone"] == phone:
                my_rank = {
                    "rank":   i + 1,
                    "points": round(r["points"], 1),
                }
                break
        response["me"] = my_rank  # None ถ้าเบอร์นี้ยังไม่มี point เดือนนี้

        # point สะสมทั้งหมดตลอดกาล (ไม่กรองตามเดือน)
        all_rows = sb.table("point_ledger") \
            .select("points") \
            .eq("phone", phone) \
            .execute()
        total_all = sum(float(r.get("points") or 0) for r in (all_rows.data or []))
        response["total_points_all_time"] = round(total_all, 1)

    return response


class LineOAuthRequest(BaseModel):
    code:         str
    redirect_uri: str

@app.post("/auth/line-oauth")
async def line_oauth(body: LineOAuthRequest):
    """แลก LINE OAuth code เป็น profile — ทำฝั่ง server เพราะต้องใช้ Channel Secret"""
    LINE_CHANNEL_ID     = os.getenv("LINE_CHANNEL_ID", "2010290578")
    LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")

    # แลก code เป็น access token
    async with httpx.AsyncClient(timeout=10) as client:
        token_res = await client.post(
            "https://api.line.me/oauth2/v2.1/token",
            data={
                "grant_type":    "authorization_code",
                "code":          body.code,
                "redirect_uri":  body.redirect_uri,
                "client_id":     LINE_CHANNEL_ID,
                "client_secret": LINE_CHANNEL_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if token_res.status_code != 200:
            raise HTTPException(status_code=400, detail=f"LINE token error: {token_res.text}")
        token_data = token_res.json()
        access_token = token_data.get("access_token")

        # ดึง profile
        profile_res = await client.get(
            "https://api.line.me/v2/profile",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if profile_res.status_code != 200:
            raise HTTPException(status_code=400, detail="LINE profile error")
        profile = profile_res.json()

    # upsert ลง customers
    sb = get_supabase()
    sb.table("customers").upsert({
        "line_user_id": profile["userId"],
        "display_name": profile.get("displayName"),
        "picture_url":  profile.get("pictureUrl"),
    }, on_conflict="line_user_id").execute()

    # อัปเดต picture_url แยกเพื่อให้แน่ใจว่าอัปเดตจริง (upsert อาจ skip ถ้า row มีอยู่แล้ว)
    sb.table("customers").update({
        "display_name": profile.get("displayName"),
        "picture_url":  profile.get("pictureUrl"),
    }).eq("line_user_id", profile["userId"]).execute()

    res = sb.table("customers").select("*").eq("line_user_id", profile["userId"]).execute()
    customer = res.data[0] if res.data else None

    # ลูกค้าที่ login LINE → ตั้งรับแจ้งเตือนทาง LINE (เฉพาะถ้ายังไม่เคยตั้งค่าไว้ เคารพคนที่เลือกปิด/SMS เอง)
    if customer and not customer.get("notify_channel"):
        try:
            sb.table("customers").update({"notify_channel": "line"}).eq("line_user_id", profile["userId"]).execute()
            customer["notify_channel"] = "line"
        except Exception as e:
            print(f"[line-oauth] set notify_channel error: {e}")

    return {"success": True, "customer": customer}
