import os
import re
import time
import httpx
import asyncio
from typing import Optional
from datetime import datetime, timedelta, date
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
    """ส่งข้อความผ่าน LINE OA — retry ถ้าเจอ error ชั่วคราว (5xx/timeout) สูงสุด 3 ครั้ง"""
    token = os.getenv("LINE_CHANNEL_TOKEN", "")
    if not token:
        print("[LINE] ยังไม่ได้ตั้ง LINE_CHANNEL_TOKEN")
        return False

    MAX_ATTEMPTS = 3
    success = False
    last_detail = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.post(
                    "https://api.line.me/v2/bot/message/push",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"to": line_user_id, "messages": [{"type": "text", "text": message}]}
                )
            if resp.status_code == 200:
                success = True
                print(f"[LINE] ✓ ส่งหา {line_user_id[:8]}... สำเร็จ" + (f" (ครั้งที่ {attempt})" if attempt > 1 else ""))
                break
            last_detail = f"{resp.status_code} {resp.text}"
            # 4xx = error ถาวร (token/รูปแบบผิด) ไม่ต้อง retry | 5xx = LINE ล่มชั่วคราว → ลองใหม่
            if resp.status_code < 500:
                print(f"[LINE] ✗ {last_detail} (client error — ไม่ retry)")
                break
            print(f"[LINE] ✗ {last_detail} (ครั้งที่ {attempt}/{MAX_ATTEMPTS})")
        except Exception as e:
            last_detail = str(e)
            print(f"[LINE] ERROR ครั้งที่ {attempt}/{MAX_ATTEMPTS}: {e}")
        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(attempt)   # backoff 1s, 2s

    if not success and last_detail:
        print(f"[LINE] ✗ ยอมแพ้หลังลอง {MAX_ATTEMPTS} ครั้ง: {last_detail}")

    # log ลง sms_logs (ครั้งเดียวหลังจบ retry)
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


async def send_sms(phone: str, message: str, barcode: str = "", status: str = "", customer: str = "", force: bool = False):
    """ส่ง SMS ผ่าน Thaibulksms พร้อม log (force=True ข้าม dedup ให้ส่งซ้ำได้)"""
    if not SMS_API_KEY or not SMS_API_SECRET:
        print(f"[SMS] ยังไม่ได้ตั้ง SMS key")
        return False
    if not phone or len(phone) < 9:
        print(f"[SMS] เบอร์โทรไม่ถูกต้อง: {phone}")
        return False

    # เช็คว่าเคยส่ง status นี้ไปแล้วหรือยัง (เฉพาะกรณีมี barcode และไม่ force)
    if barcode and status and not force:
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


# ช่องที่ลูกค้าเลือก "ปิด" แจ้งเตือนไว้ชัดเจน — เท่านั้นที่จะไม่ส่ง LINE
_NOTIFY_OPT_OUT = {"off", "none", "disabled", "mute", "muted"}

def _resolve_notify_channel(notify_ch, line_uid) -> str:
    """เลือกช่องแจ้งเตือน:
    - ถ้ามี line_user_id → ใช้ LINE เสมอ (notify_channel='sms' ที่ติดมาจาก default เดิม
      ไม่ถือว่าเป็นการเลือกปิด LINE — ลูกค้าที่ login LINE ต้องได้แจ้งเตือนทาง LINE)
    - ยกเว้นลูกค้าตั้งค่าปิดแจ้งเตือนไว้ชัดเจน (off/none/...) → คืน 'off'
    - ไม่มี line_user_id → 'sms'
    """
    ch = (notify_ch or "").strip().lower()
    if ch in _NOTIFY_OPT_OUT:
        return "off"
    if line_uid:
        return "line"
    return "sms"

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
        channel = _resolve_notify_channel(notify_ch, line_uid)
        if channel == "line" and line_uid:
            await send_line_notify(line_uid, message, barcode=order_id, status=status_tag, customer=customer, phone=phone)
            return True
    except Exception as e:
        print(f"[customer-notify] {status_tag} error: {e}")
    return False


async def _notify_customer(sb, order_id: str, phone: str, customer: str,
                           line_message: str, sms_message: str, status_tag: str) -> str:
    """แจ้งลูกค้าแบบมี fallback:
    - มี line_user_id และไม่ได้ปิดแจ้งเตือน → ส่ง LINE
    - ไม่มี LINE (ลูกค้า login ด้วยเบอร์/OTP) → ส่ง SMS แทน (force=True กันโดน dedup)
    - ปิดแจ้งเตือนไว้ชัดเจน → ไม่ส่ง
    คืนช่องที่ส่งจริง: 'line' / 'sms' / 'off' / '' (ส่งไม่ได้)
    """
    line_uid  = ""
    notify_ch = None
    try:
        if phone:
            look = sb.table("customers").select("line_user_id,notify_channel").eq("phone", phone).execute()
            if look.data:
                line_uid  = look.data[0].get("line_user_id") or ""
                notify_ch = look.data[0].get("notify_channel")
        channel = _resolve_notify_channel(notify_ch, line_uid)
        if channel == "off":
            print(f"[customer-notify] {status_tag} ข้าม {customer} → ปิดแจ้งเตือน")
            return "off"
        if channel == "line" and line_uid:
            ok_line = await send_line_notify(line_uid, line_message, barcode=order_id, status=status_tag, customer=customer, phone=phone)
            if ok_line:
                return "line"
            # LINE ส่งไม่สำเร็จ (เช่น token เพิก/LINE 5xx หลัง retry) → ตกไป SMS แทน ไม่ให้ลูกค้าพลาดการแจ้ง
            print(f"[customer-notify] {status_tag} LINE ล้มเหลว → ตก SMS ({customer})")
        # fallback → SMS (ไม่มี LINE หรือ LINE ล้มเหลว)
        ph = (phone or "").strip()
        if ph and ph != "-" and len(ph) >= 9:
            ok = await send_sms(ph, sms_message, barcode=order_id, status=status_tag, customer=customer, force=True)
            return "sms" if ok else ""
    except Exception as e:
        print(f"[customer-notify] {status_tag} error: {e}")
    return ""

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
    # เลขขึ้นต้น SPX = Shopee Xpress ชัดเจน — เข้ากลุ่ม flash-express ที่จะ "ลอง SPX ก่อน"
    if re.match(r'^SPX', b):               return "flash-express"
    # Flash Express (Shopee-managed) ใช้เลข THxxxxxxxxxxC — แต่ SPX ก็ใช้ TH เหมือนกัน
    # → เข้ากลุ่ม flash-express แล้วให้ fetch_tracking/cron ลอง SPX ก่อน ถ้าไม่ใช่ค่อย Flash
    if re.match(r'^TH', b):                 return "flash-express"
    if re.match(r'^(SXF|SCPK)', b):        return "kex-express"
    if re.match(r'^(FLE|FEX)', b):         return "flash-express"
    # eTrackings ถูกลบออกแล้ว (ไม่ใช้บริการเสียเงินนี้แล้ว) — เลข TDE/JPT/JTTH/SCG จะตกไป thailand_post
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

    # diagnostic: โควตา track รายวัน + ได้ข้อมูลกี่เลขจาก batch นี้
    # (ถ้าได้ 0 เลข + count ใกล้ limit = โควตาเต็ม | ถ้าบางเลขได้บางเลขไม่ได้ = พัสดุนั้นยังไม่เข้า API)
    _got = sum(1 for b in barcodes if items.get(b))
    _empty = [b for b in barcodes if not items.get(b)]
    print(f"[thaipost-api] เช็ค {len(barcodes)} เลข · ได้ข้อมูล {_got} · "
          f"track_count={track_count.get('count_number')}/{track_count.get('track_count_limit')}"
          + (f" · ไม่มีข้อมูล: {_empty}" if _empty else ""))

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


# ── SPX (Shopee Express) — เรียก open endpoint ของ spx.co.th ตรง (ไม่ต้อง scraper/ไม่ต้อง auth) ──
SPX_TRACK_URL = "https://spx.co.th/shipment/order/open/order/get_order_info"
# milestone_code ของ SPX → สถานะระบบเรา
_SPX_MILESTONE = {
    1: ("accepted",         "เตรียมจัดส่ง"),
    2: ("accepted",         "รับพัสดุแล้ว"),
    3: ("in_transit",       "อยู่ระหว่างขนส่ง"),
    4: ("in_transit",       "อยู่ระหว่างขนส่ง"),
    5: ("in_transit",       "อยู่ระหว่างขนส่ง"),
    6: ("out_for_delivery", "กำลังนำจ่าย"),
    8: ("delivered",        "จัดส่งสำเร็จ"),
}

async def fetch_spx(barcode: str) -> Optional[dict]:
    """ดึงสถานะพัสดุ SPX (Shopee Express) จาก open endpoint ของ spx.co.th
    คืน None ถ้าไม่ใช่พัสดุ SPX (ไม่มี records) → ให้ caller ลอง Flash ต่อ
    (เลข TH ใช้ร่วมกันทั้ง SPX และ Flash แยกจากหัวเลขไม่ได้ จึงลอง SPX ก่อน)"""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(SPX_TRACK_URL,
                                  params={"spx_tn": barcode, "language_code": "th"},
                                  headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        j = r.json()
        recs = (((j.get("data") or {}).get("sls_tracking_info") or {}).get("records")) or []
        if not recs:
            return None   # ไม่ใช่พัสดุ SPX (หรือ 3PL) → ให้ caller ลอง Flash/SPX-scraper ต่อ
    except Exception as e:
        print(f"[SPX] error {barcode}: {e}")
        return None

    events = []
    for e in recs:
        ms = e.get("milestone_code")
        st, th = _SPX_MILESTONE.get(ms, ("in_transit", e.get("milestone_name") or "อยู่ระหว่างขนส่ง"))
        name = ((e.get("tracking_name") or "") + " " + (e.get("description") or ""))
        low  = name.lower()
        if "return" in low or "ตีกลับ" in name or "ส่งคืน" in name:
            st, th = "returned", "ตีกลับ/ส่งคืนต้นทาง"
        elif "เสียหาย" in name or "ชำรุด" in name or "คืนเงิน" in name or "damage" in low or "refund" in low:
            st, th = "problem", (e.get("description") or "พัสดุเสียหาย/คืนเงินผู้ซื้อ")
        elif "fail" in low or "unsuccess" in low or "ไม่สำเร็จ" in name:
            st, th = "problem", "นำจ่ายไม่สำเร็จ"
        dt = ""
        t  = e.get("actual_time")
        if t:
            try:
                dt = datetime.utcfromtimestamp(int(t) + 7 * 3600).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dt = ""
        # SPX คืน current_location เป็น object {location_name, full_address, lng, lat, ...}
        # ต้องแปลงเป็น string ก่อน ไม่งั้นหน้าเว็บ render object → React error #31 (crash ทั้งหน้า)
        loc = e.get("current_location") or ""
        if isinstance(loc, dict):
            loc = loc.get("location_name") or loc.get("full_address") or ""
        events.append({
            "status_code": str(e.get("tracking_code") or ""),
            "status":      st,
            "description": (e.get("description") or th or "").strip(),
            "datetime":    dt,
            "location":    str(loc or ""),
        })
    events.reverse()   # records[0]=ล่าสุด → เรียงเก่า→ใหม่ ให้เหมือน carrier อื่น
    latest = events[-1] if events else None
    if any(ev["status"] == "delivered" for ev in events):
        delivered  = next((ev for ev in reversed(events) if ev["status"] == "delivered"), None)
        cur_status = "delivered"
        cur_th     = (delivered or {}).get("description") or "จัดส่งสำเร็จ"
        if delivered:
            latest = delivered
    else:
        cur_status = latest["status"] if latest else "pending"
        cur_th     = latest["description"] if latest else "รอข้อมูล"
    return {
        "barcode":      barcode,
        "status":       cur_status,
        "status_th":    cur_th,
        "latest_event": latest,
        "events":       events,
        "carrier":      "SPX Express",
    }


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
        # เลข TH ใช้ร่วมกันทั้ง SPX (Shopee Xpress) และ Flash → ลอง SPX open API ก่อน (เร็ว)
        spx = await fetch_spx(barcode)
        if spx:
            return spx
        # Flash (Shopee-managed) — scraper เดียวกับ KEX (headless เปิดหน้า Flash ผ่าน 5 Second Shield)
        flash_data = None
        if KEX_SCRAPER_URL:
            try:
                async with httpx.AsyncClient(timeout=75) as client:
                    r = await client.get(
                        f"{KEX_SCRAPER_URL}/track-flash/{barcode}",
                        headers={"x-api-key": KEX_SCRAPER_KEY},
                    )
                    if r.status_code == 200:
                        data = r.json()
                        # Flash มี event จริง → คืนเลย (พัสดุ Flash ปกติ)
                        if data.get("events") and data.get("status") not in ("error", "unknown", None):
                            return data
                        flash_data = data   # pending/ไม่มี event — เก็บไว้ก่อน ลอง SPX scraper ต่อ
            except Exception as e:
                print(f"[Flash Scraper] error {barcode}: {e}")
        # Flash ไม่มี event → อาจเป็นพัสดุ SPX 3PL (open API ไม่คืน แต่หน้าเว็บ SPX มี)
        # ลอง SPX scraper (headless เปิด spx.co.th/track ดัก fleet_order/tracking/search)
        if KEX_SCRAPER_URL:
            try:
                async with httpx.AsyncClient(timeout=90) as client:
                    r = await client.get(
                        f"{KEX_SCRAPER_URL}/track-spx/{barcode}",
                        headers={"x-api-key": KEX_SCRAPER_KEY},
                    )
                    if r.status_code == 200:
                        sdata = r.json()
                        if sdata.get("events"):
                            return sdata
            except Exception as e:
                print(f"[SPX Scraper] error {barcode}: {e}")
        # ทั้ง SPX และ Flash ไม่มี event → คืนผล Flash (pending) ถ้ามี ไม่งั้น placeholder
        if flash_data and flash_data.get("status") not in ("error", "unknown", None):
            return flash_data
        return {"barcode": barcode, "status": "shopee_managed",
                "status_th": "จัดส่งโดย Flash Express (Shopee) — ติดตามในแอป Shopee",
                "events": []}
    elif carrier != "thailand_post" and ETRACKINGS_API_KEY:
        return await fetch_etrackings(barcode, carrier)
    return (await fetch_tracking_batch([barcode]))[0]


async def _send_unpaid_reminder(sb, order: dict, stage: int, expire_hours: int):
    """แจ้งเตือนออเดอร์ค้างชำระแบบบันได (stage 1=~3ชม, 2=~24ชม, 3=~72ชม) + ลิงก์กลับไปจ่าย"""
    oid      = order.get("order_id")
    phone    = (order.get("account_phone") or order.get("phone") or "").strip()
    customer = order.get("customer") or "ลูกค้า"
    total    = float(order.get("total") or 0)
    link     = f"velacoldbrew.com/order-complete?order_id={oid}"
    if stage == 1:
        line_msg = (f"VeLA Cold Brew: ออเดอร์ #{oid} (฿{total:,.0f}) + ส่วนลดของคุณจองไว้ให้แล้วค่ะ 🐰\n"
                    f"ชำระได้เลยที่ 👉 {link}")
        sms_msg  = f"VeLA Cold Brew: ออเดอร์ #{oid} ฿{total:,.0f} + ส่วนลดจองไว้ให้แล้ว ชำระที่ {link}"
    elif stage == 2:
        line_msg = (f"VeLA Cold Brew: ออเดอร์ #{oid} ยังรอชำระอยู่นะคะ 💛\n"
                    f"สิทธิ์ส่วนลดจะหมดอายุเร็วๆ นี้ ชำระเลยที่ 👉 {link}")
        sms_msg  = f"VeLA Cold Brew: ออเดอร์ #{oid} ยังรอชำระ สิทธิ์ส่วนลดใกล้หมด ชำระที่ {link}"
    else:  # stage 3
        left = max(1, expire_hours - 72)
        line_msg = (f"VeLA Cold Brew: ออเดอร์ #{oid} อีกประมาณ {left} ชม. จะถูกยกเลิกอัตโนมัติค่ะ 🙏\n"
                    f"รีบชำระที่ 👉 {link} นะคะ")
        sms_msg  = f"VeLA Cold Brew: ออเดอร์ #{oid} อีก ~{left} ชม.จะถูกยกเลิก รีบชำระที่ {link}"
    await _notify_customer(sb, oid, phone, customer, line_msg, sms_msg, f"payment_reminder_{stage}")


async def run_cron():
    """เช็คเฉพาะพัสดุที่ is_done = false ทุก 3 ชั่วโมง เฉพาะช่วง 10:00-18:00"""

    print("[cron] เริ่มเช็คสถานะพัสดุที่ยังไม่เสร็จ...")
    sb = get_supabase()

    # ลบ order เว็บที่รอชำระเกินเวลา — ใช้ created_at ถ้ามี ไม่งั้น fallback ไป order_date
    # (บั๊กเดิม: order เก่า created_at เป็น NULL ตัวกรอง .lt(created_at) เลยไม่เจอ = ไม่ลบ)
    try:
        expire_hours = int(os.getenv("ORDER_EXPIRE_HOURS", "96"))   # 4 วัน (ปรับผ่าน env, เทศกาลยืดได้)
        now      = datetime.utcnow()
        date_cutoff = (now - timedelta(hours=expire_hours) - timedelta(days=1)).date()
        thai_hour = (now + timedelta(hours=7)).hour
        can_remind = 9 <= thai_hour <= 20   # ส่งแจ้งเตือนเฉพาะกลางวัน (กันรบกวนตอนดึก)

        pending = sb.table("orders") \
            .select("order_id,created_at,order_date,phone,customer,total,first_order_discount,reminder_stage,account_phone,line_user_id") \
            .eq("status", "รอชำระเงิน") \
            .eq("channel", "web") \
            .execute()

        expired_ids = []
        expired_phones = set()   # เบอร์ที่ต้องคืนสิทธิ์ลูกค้าใหม่ 50%
        for r in (pending.data or []):
            ca = r.get("created_at"); od = r.get("order_date")
            age_hours = None
            if ca:
                try:
                    cadt = datetime.fromisoformat(str(ca).replace("Z", "+00:00")).replace(tzinfo=None)
                    age_hours = (now - cadt).total_seconds() / 3600.0
                except Exception:
                    age_hours = None

            # หมดเวลา → เก็บไว้ลบ
            old = age_hours >= expire_hours if age_hours is not None else False
            if old is False and age_hours is None and od:
                try:
                    old = datetime.strptime(str(od)[:10], "%Y-%m-%d").date() <= date_cutoff
                except Exception:
                    old = False
            if old:
                expired_ids.append(r["order_id"])
                if r.get("first_order_discount"):
                    ph = (r.get("account_phone") or r.get("phone") or "").strip()
                    if ph:
                        expired_phones.add(ph)
                continue

            # ยังไม่หมดเวลา → แจ้งเตือนแบบบันได (3 → 24 → 72 ชม.) เฉพาะกลางวัน
            if age_hours is None or not can_remind:
                continue
            stage = int(r.get("reminder_stage") or 0)
            target = stage
            if   age_hours >= 72 and stage < 3: target = 3
            elif age_hours >= 24 and stage < 2: target = 2
            elif age_hours >= 3  and stage < 1: target = 1
            if target != stage:
                try:
                    await _send_unpaid_reminder(sb, r, target, expire_hours)
                    sb.table("orders").update({"reminder_stage": target}).eq("order_id", r["order_id"]).execute()
                    print(f"[cron] แจ้งเตือนค้างชำระ #{r['order_id']} ระดับ {target} (อายุ {age_hours:.0f} ชม.)")
                except Exception as e:
                    print(f"[cron] reminder error {r['order_id']}: {e}")

        if expired_ids:
            # ลบลูก (accounting) ก่อนพ่อ (orders) — ไม่งั้น FK accounting_order_id_fkey บล็อก
            try:
                sb.table("accounting").delete().in_("order_id", expired_ids).execute()
            except Exception as e:
                print(f"[cron] ลบ accounting ค้างชำระ error: {e}")
            try:
                sb.table("shipping").delete().in_("order_id", expired_ids).execute()
            except Exception as e:
                print(f"[cron] ลบ shipping ค้างชำระ error: {e}")
            sb.table("orders").delete().in_("order_id", expired_ids).execute()
            print(f"[cron] ลบ order ค้างชำระหมดเวลา {len(expired_ids)} รายการ: {expired_ids}")
            # คืนสิทธิ์ลูกค้าใหม่ 50% ให้เบอร์ที่ออเดอร์ถูกลบ — กลับมาสั่งใหม่ยังได้ส่วนลดเหมือนเดิม
            # (ปลอดภัย: _is_first_order_eligible ยังเช็ค 'ไม่เคยมีออเดอร์เว็บ' อีกชั้น กันคนเคยจ่ายจริง)
            for ph in expired_phones:
                try:
                    sb.table("customers").update({"first_order_used": False}).eq("phone", ph).execute()
                    print(f"[cron] คืนสิทธิ์ลูกค้าใหม่ 50% → ...{ph[-4:]}")
                except Exception as e:
                    print(f"[cron] คืนสิทธิ์ error {ph}: {e}")
        else:
            print(f"[cron] ไม่มี order ค้างชำระหมดเวลา (รอชำระ web ทั้งหมด {len(pending.data or [])} รายการ)")
    except Exception as e:
        print(f"[cron] expire error: {e}")

    rows = sb.table("shipments").select("barcode").eq("is_done", False).execute()
    barcodes = [r["barcode"] for r in (rows.data or []) if r.get("barcode") and str(r["barcode"]).strip()]
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

    # แยก SPX ออกจาก Flash ก่อน — เลข TH ใช้ร่วมกันทั้ง SPX (Shopee Xpress) และ Flash
    # ลอง SPX open endpoint ทีละเลข (ฟรี เร็ว) ถ้าใช่ SPX เก็บผลเลย ที่เหลือค่อยส่ง Flash scraper
    if flash_barcodes:
        still_flash = []
        for b in flash_barcodes:
            spx = await fetch_spx(b)
            if spx:
                all_results.append({
                    "barcode":      b,
                    "status":       spx.get("status", "unknown"),
                    "status_th":    spx.get("status_th", ""),
                    "latest_event": spx.get("latest_event") or {},
                    "events":       spx.get("events", []),
                })
                print(f"[cron] SPX {b} → {spx.get('status')}")
            else:
                still_flash.append(b)
        flash_barcodes = still_flash

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
                    for b in batch:   # scraper ล่ม → คงเลขไว้เป็น pending (ไม่ทิ้ง) ให้ SPX fallback ลองต่อ + ไม่ทับข้อมูลเดิม
                        all_results.append({"barcode": b, "status": "pending", "status_th": "รอข้อมูล", "latest_event": {}, "events": []})
        except Exception as e:
            print(f"[Flash Scraper] bulk error batch {i}: {e}")
            for b in batch:
                all_results.append({"barcode": b, "status": "pending", "status_th": "รอข้อมูล", "latest_event": {}, "events": []})

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

    # SPX 3PL fallback — พัสดุ TH ที่ยังค้าง pending/ไม่มี event (open API SPX ไม่เจอ + Flash ไม่มีข้อมูล)
    # ลองดึงจากหน้าเว็บ SPX ผ่าน scraper (headless ดัก fleet_order/tracking/search) — ครอบคลุมพัสดุ 3PL
    if KEX_SCRAPER_URL:
        _spx_retry = [str(r.get("barcode", "")).upper() for r in all_results
                      if str(r.get("barcode", "")).upper().startswith("TH")
                      and not r.get("events")
                      and r.get("status") in ("pending", "unknown", "shopee_managed", None)]
        if _spx_retry:
            print(f"[cron] SPX scraper fallback: {len(_spx_retry)} เลข → {', '.join(_spx_retry)}")
            try:
                async with httpx.AsyncClient(timeout=240) as client:
                    rr = await client.post(f"{KEX_SCRAPER_URL}/track-spx/bulk",
                                           headers={"x-api-key": KEX_SCRAPER_KEY},
                                           json={"tns": _spx_retry})
                if rr.status_code == 200:
                    sdata = rr.json().get("results", {})
                    for r in all_results:
                        b = str(r.get("barcode", "")).upper()
                        sres = sdata.get(b)
                        if sres and sres.get("events"):
                            r["status"]       = sres.get("status", r.get("status"))
                            r["status_th"]    = sres.get("status_th", r.get("status_th"))
                            _ev = sres.get("events") or []
                            r["latest_event"] = {"location": sres.get("latest_location", ""),
                                                 "datetime": (_ev[-1] if _ev else {}).get("datetime", "")}
                            r["events"]       = _ev
                            print(f"[cron] SPX-scraper {b} → {r['status']} ({len(_ev)} events)")
            except Exception as e:
                print(f"[cron] SPX scraper error: {e}")

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
        # พัสดุเริ่มเคลื่อนไหว (ขนส่ง scan รับพัสดุจริง) → ออเดอร์ที่ยัง "เตรียมจัดส่ง" เลื่อนเป็น "จัดส่งแล้ว"
        elif status in ("accepted", "in_transit", "out_for_delivery"):
            try:
                sr = sb.table("shipping").select("order_id").eq("tracking", barcode).execute()
                if sr.data:
                    oid = sr.data[0]["order_id"]
                    cur = sb.table("orders").select("status").eq("order_id", oid).execute()
                    if cur.data and cur.data[0].get("status") == "เตรียมจัดส่ง":
                        sb.table("orders").update({"status": "จัดส่งแล้ว"}).eq("order_id", oid).execute()
                        print(f"[cron] link → order {oid} = จัดส่งแล้ว (พัสดุเคลื่อนไหว)")
            except Exception as e:
                print(f"[cron] link ship→order status error: {e}")

        # แจ้ง admin ทันทีถ้าพัสดุมีปัญหา (เสียหาย/ตีกลับ/นำจ่ายไม่สำเร็จ) — ยิงเสมอ ไม่ผูกกับ template ลูกค้า
        # (เดิมบล็อกนี้ซ้อนอยู่ใต้ has_notify ซึ่ง problem/returned = None เลยไม่เคยแจ้งจริง)
        if status != old_status and status in ALERT_STATUSES and ADMIN_LINE_USER_ID:
            try:
                _sr = sb.table("shipping").select("order_id").eq("tracking", barcode).execute()
                _oid = _sr.data[0]["order_id"] if _sr.data else ""
                _cname, _cphone = "", ""
                if _oid:
                    _or = sb.table("orders").select("customer,phone").eq("order_id", _oid).execute()
                    if _or.data:
                        _cname  = _or.data[0].get("customer") or ""
                        _cphone = _or.data[0].get("phone") or ""
                await send_line_notify(ADMIN_LINE_USER_ID,
                    "⚠️ พัสดุมีปัญหา — โปรดแจ้งลูกค้า\n"
                    f"ลูกค้า: {_cname}" + (f" ({_cphone})" if _cphone else "") +
                    (f"\nOrder: {_oid}" if _oid else "") +
                    f"\nเลขพัสดุ: {barcode}\n"
                    f"สถานะ: {result.get('status_th') or status}\n"
                    f"ติดตาม: velacoldbrew.com/track/{barcode}")
                print(f"[ADMIN] แจ้ง admin (ปัญหา) → {barcode} {status}")
            except Exception as e:
                print(f"[ADMIN] alert error {barcode}: {e}")

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
                                # ลูกค้าที่ผูก LINE ไว้ → รับแจ้งเตือนทาง LINE เสมอ (เว้นตั้งค่าปิดชัดเจน)
                                notify   = _resolve_notify_channel(cust.data[0].get("notify_channel"), line_uid)
                        except:
                            pass

                        final_msg = msg.replace("{barcode}", barcode)

                        if notify == "off":
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
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
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
async def get_products(show_all: bool = False):
    """ดึงสินค้า พร้อมราคาหลังลด
    - ปกติ (หน้าเว็บลูกค้า): เฉพาะ active=True (สินค้าหมดก็ยังส่งมาด้วย เพื่อโชว์ว่า 'หมด')
    - show_all=1 (หน้า admin): ส่งทุกตัวรวมที่ปิดขายแล้ว เพื่อให้กดเปิดกลับได้"""
    sb = get_supabase()
    q = sb.table("products").select("*")
    if not show_all:
        q = q.eq("active", True)
    res = q.order("sort_order").execute()
    products = []
    for p in (res.data or []):
        price      = int(p.get("price") or 0)
        disc_pct   = int(p.get("discount_pct") or 0)
        disc_price = round(price * (1 - disc_pct / 100))
        _instock   = p.get("in_stock")
        products.append({
            **p,
            "price":          price,
            "price_original": price,
            "price_discounted": disc_price,
            "discount_pct":   disc_pct,
            "discount_amount": price - disc_price,
            "in_stock":       True if _instock is None else bool(_instock),   # ไม่มีค่า = ถือว่ามีของ
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


# ── ส่วนลดลูกค้า VIP (ตั้ง % ต่อคนในหน้าจัดการลูกค้า) — ไม่มีเพดาน ──
def _vip_discount(subtotal: float, pct) -> int:
    """ยอดส่วนลด VIP (บาท) = subtotal * pct% เต็ม (ไม่มีเพดาน) — สูตรเดียวกับ frontend (promo.ts)"""
    try:
        pct = int(pct or 0)
    except (TypeError, ValueError):
        pct = 0
    if subtotal <= 0 or pct <= 0:
        return 0
    return int(round(subtotal * pct / 100.0))

def _get_vip_pct(sb, phone: str = "", line_user_id: str = "") -> int:
    """ดึง vip_discount_pct ของลูกค้า — หาจาก line_user_id ก่อน แล้วค่อยเบอร์ (คนสั่ง/login)"""
    try:
        if line_user_id:
            c = sb.table("customers").select("vip_discount_pct").eq("line_user_id", line_user_id).execute()
            if c.data and c.data[0].get("vip_discount_pct"):
                return int(c.data[0]["vip_discount_pct"])
        phone = (phone or "").strip()
        if phone:
            c = sb.table("customers").select("vip_discount_pct").eq("phone", phone).execute()
            if c.data and c.data[0].get("vip_discount_pct"):
                return int(c.data[0]["vip_discount_pct"])
    except Exception as e:
        print(f"[vip] get pct error: {e}")
    return 0


@app.get("/products/check-first-order")
async def check_first_order(phone: str, line_user_id: str = ""):
    """เช็คสิทธิ์ส่วนลดของลูกค้าเบอร์นี้: โปรลูกค้าใหม่ 50% (ครั้งแรก) + ส่วนลด VIP (ถ้ามี)
    ส่ง line_user_id มาด้วยเพื่อให้ VIP ที่ผูกกับ LINE โชว์บนหน้า checkout ตรงกับตอนคิดเงินจริง"""
    sb = get_supabase()
    vip_pct  = _get_vip_pct(sb, phone, line_user_id)
    eligible = _is_first_order_eligible(sb, phone) if _promo_first_order_enabled() else False
    return {
        "eligible":         eligible,
        "discount_pct":     int(FIRST_ORDER_PCT * 100) if eligible else 0,
        "vip_discount_pct": vip_pct,
        "cap":              int(FIRST_ORDER_CAP),
    }


# ── ที่อยู่จัดส่งของลูกค้า (ผ่าน backend + service key — ปิด anon เข้า addresses ตรงได้) ──
class AddressBody(BaseModel):
    phone:        str
    name:         Optional[str] = None
    full_address: Optional[str] = None
    subdistrict:  Optional[str] = None
    district:     Optional[str] = None
    province:     Optional[str] = None
    zip:          Optional[str] = None
    is_default:   bool = False
    customer_id:  Optional[int] = None

class AddressUpdateBody(BaseModel):
    phone:        str            # ใช้ scope การ unset default + ยืนยันความเป็นเจ้าของ
    name:         Optional[str] = None
    full_address: Optional[str] = None
    subdistrict:  Optional[str] = None
    district:     Optional[str] = None
    province:     Optional[str] = None
    zip:          Optional[str] = None
    is_default:   Optional[bool] = None

@app.get("/addresses")
async def list_addresses(phone: str, x_auth_token: str = Header(default="")):
    ph = (phone or "").strip()
    if not ph:
        return {"addresses": []}
    _check_customer(x_auth_token, phone=ph)
    sb = get_supabase()
    res = sb.table("addresses").select("*").eq("phone", ph) \
        .order("is_default", desc=True).order("id", desc=True).execute()
    return {"addresses": res.data or []}

@app.post("/addresses")
async def add_address(body: AddressBody, x_auth_token: str = Header(default="")):
    ph = (body.phone or "").strip()
    if len(ph) < 9:
        raise HTTPException(status_code=400, detail="เบอร์โทรไม่ถูกต้อง")
    _check_customer(x_auth_token, phone=ph)
    sb = get_supabase()
    cnt = sb.table("addresses").select("id").eq("phone", ph).execute()
    if len(cnt.data or []) >= 3:
        raise HTTPException(status_code=400, detail="เก็บที่อยู่ได้สูงสุด 3 รายการ")
    if body.is_default:
        sb.table("addresses").update({"is_default": False}).eq("phone", ph).execute()
    row = {k: v for k, v in {
        "phone": ph, "name": body.name, "full_address": body.full_address,
        "subdistrict": body.subdistrict, "district": body.district,
        "province": body.province, "zip": body.zip,
        "is_default": body.is_default, "customer_id": body.customer_id,
    }.items() if v is not None}
    ins = sb.table("addresses").insert(row).execute()
    return {"success": True, "address": ins.data[0] if ins.data else None}

@app.patch("/addresses/{addr_id}")
async def update_address(addr_id: int, body: AddressUpdateBody, x_auth_token: str = Header(default="")):
    ph = (body.phone or "").strip()
    _check_customer(x_auth_token, phone=ph)
    sb = get_supabase()
    # ยืนยันว่าที่อยู่นี้เป็นของเบอร์นี้จริง (กันแก้ของคนอื่นด้วยการเดา id)
    own = sb.table("addresses").select("id").eq("id", addr_id).eq("phone", ph).execute()
    if not own.data:
        raise HTTPException(status_code=404, detail="ไม่พบที่อยู่")
    if body.is_default:
        sb.table("addresses").update({"is_default": False}).eq("phone", ph).execute()
    upd = {k: v for k, v in body.model_dump().items() if k != "phone" and v is not None}
    if upd:
        sb.table("addresses").update(upd).eq("id", addr_id).execute()
    return {"success": True}

@app.delete("/addresses/{addr_id}")
async def delete_address(addr_id: int, phone: str, x_auth_token: str = Header(default="")):
    ph = (phone or "").strip()
    _check_customer(x_auth_token, phone=ph)
    sb = get_supabase()
    own = sb.table("addresses").select("id").eq("id", addr_id).eq("phone", ph).execute()
    if not own.data:
        raise HTTPException(status_code=404, detail="ไม่พบที่อยู่")
    sb.table("addresses").delete().eq("id", addr_id).execute()
    return {"success": True}


# ============================================================
#  Customer session token — พิสูจน์ว่าคนเรียกเป็นเจ้าของ phone/line จริง
#  ออกตอน login (OTP/LINE) แล้ว endpoint ข้อมูลส่วนตัวเช็ค token + เจ้าของต้องตรง
#  ENFORCE_CUSTOMER_TOKEN=0 (default) = โหมด grace ยังไม่บังคับ (rollout ปลอดภัย)
#  ตั้ง =1 บน Render เมื่อพร้อมบังคับ (ลูกค้าเก่าต้อง login ใหม่ 1 ครั้ง)
# ============================================================
import hmac as _hmac2, hashlib as _hashlib2, base64 as _base64_2
_CUSTOMER_SECRET = (os.getenv("CUSTOMER_SECRET") or os.getenv("ADMIN_SECRET") or os.getenv("ADMIN_API_KEY") or "").encode()
CUSTOMER_TOKEN_TTL = int(os.getenv("CUSTOMER_TOKEN_TTL", str(45 * 24 * 3600)))   # 45 วัน
ENFORCE_CUSTOMER_TOKEN = os.getenv("ENFORCE_CUSTOMER_TOKEN", "0") == "1"

def _norm_phone(p: str) -> str:
    return (p or "").replace("-", "").replace(" ", "").strip()

def _make_customer_token(phone: str = "", line_user_id: str = "", exp: int = 0) -> str:
    exp = exp or (int(time.time()) + CUSTOMER_TOKEN_TTL)
    ph = _norm_phone(phone); lid = (line_user_id or "").strip()
    sig = _base64_2.urlsafe_b64encode(
        _hmac2.new(_CUSTOMER_SECRET, f"c1.{ph}.{lid}.{exp}".encode(), _hashlib2.sha256).digest()
    ).decode().rstrip("=")
    return f"c1.{ph}.{lid}.{exp}.{sig}"

def _verify_customer_token(tok: str):
    if not tok or not _CUSTOMER_SECRET:
        return None
    parts = tok.split(".")
    if len(parts) != 5 or parts[0] != "c1":
        return None
    try:
        exp = int(parts[3])
    except ValueError:
        return None
    if exp < int(time.time()):
        return None
    if not _hmac2.compare_digest(tok, _make_customer_token(parts[1], parts[2], exp)):
        return None
    return {"phone": parts[1], "line_user_id": parts[2]}

def _check_customer(token: str, phone: str = None, line_user_id: str = None):
    """เช็คว่าลูกค้าที่เรียกเป็นเจ้าของข้อมูลจริง (grace mode = ไม่บังคับจนกว่าจะ ENFORCE)"""
    if not ENFORCE_CUSTOMER_TOKEN:
        return
    ctx = _verify_customer_token(token)
    if not ctx:
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบใหม่")
    if phone and (ctx.get("phone") or "") != _norm_phone(phone):
        raise HTTPException(status_code=403, detail="ไม่มีสิทธิ์เข้าถึงข้อมูลนี้")
    if line_user_id and (ctx.get("line_user_id") or "") != line_user_id.strip():
        raise HTTPException(status_code=403, detail="ไม่มีสิทธิ์เข้าถึงข้อมูลนี้")


# ============================================================
#  Customer self-service reads / profile
#  ยิงผ่าน backend + service key + customer token (แทน anon key ตรง)
# ============================================================

_MY_ORDER_FIELDS = ("order_id,order_date,ship_date,customer,phone,sku,qty,total,status,"
                    "province,zip,full_address,note,channel,slip_url,slip_status,paid_at,created_at")

def _join_tracking(sb, orders):
    """เติมเลขพัสดุ/ขนส่งจากตาราง shipping ให้ลิสต์ออเดอร์ (in-place)"""
    oids = [o["order_id"] for o in orders if o.get("order_id")]
    tmap = {}
    if oids:
        try:
            sh = sb.table("shipping").select("order_id,tracking,carrier").in_("order_id", oids).execute()
            for row in (sh.data or []):
                if row.get("order_id") and row["order_id"] not in tmap:
                    tmap[row["order_id"]] = {"tracking": row.get("tracking"), "carrier": row.get("carrier")}
        except Exception as e:
            print(f"[orders] shipping join error: {e}")
    for o in orders:
        t = tmap.get(o.get("order_id")) or {}
        o["tracking"] = t.get("tracking")
        o["carrier"]  = t.get("carrier")
    return orders

@app.get("/my/orders")
async def my_orders(phone: str, limit: int = 20, x_auth_token: str = Header(default="")):
    """ออเดอร์ของเบอร์นี้ (+ เลขพัสดุ) — หน้า account / ประวัติหลัง login"""
    ph = (phone or "").strip()
    if not ph:
        raise HTTPException(status_code=400, detail="ต้องระบุ phone")
    _check_customer(x_auth_token, phone=ph)
    sb = get_supabase()
    res = (sb.table("orders").select(_MY_ORDER_FIELDS)
           .eq("phone", ph).order("order_date", desc=True)
           .limit(min(int(limit or 20), 50)).execute())
    orders = res.data or []
    _join_tracking(sb, orders)
    return {"orders": orders, "count": len(orders)}

# field ที่ปลอดภัยจะโชว์ผ่านลิงก์ order-complete (เปิดด้วย order_id อย่างเดียว ไม่มี auth)
# → ตัด PII ออก (ชื่อ/เบอร์/ที่อยู่/สลิป) กัน IDOR: ใครเดา/ได้ลิงก์ไปก็เห็นแค่รายการ+ยอด+สถานะ
_PUBLIC_ORDER_FIELDS = ("order_id,order_date,ship_date,sku,qty,total,status,"
                        "channel,slip_status,paid_at,created_at")

@app.get("/my/order/{order_id}")
async def my_order(order_id: str):
    """ออเดอร์เดียวตาม order_id — หน้า order-complete / สถานะหลังสั่ง
    คืนเฉพาะข้อมูลที่ไม่ใช่ PII (หน้านี้เปิดด้วย order_id อย่างเดียว ไม่มีการยืนยันตัวตน)"""
    oid = (order_id or "").strip()
    if not oid:
        raise HTTPException(status_code=400, detail="ต้องระบุ order_id")
    sb = get_supabase()
    res = sb.table("orders").select(_PUBLIC_ORDER_FIELDS).eq("order_id", oid).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="ไม่พบออเดอร์")
    return {"order": res.data[0]}

_CUSTOMER_PUBLIC_FIELDS = ("id,line_user_id,display_name,picture_url,phone,name,"
                           "address,province,zip,notify_channel")

@app.get("/customers/by-line/{line_user_id}")
async def customer_by_line(line_user_id: str, x_auth_token: str = Header(default="")):
    """ดึงข้อมูลลูกค้าด้วย LINE user id (แทน fetchCustomer เดิม)"""
    lid = (line_user_id or "").strip()
    if not lid:
        raise HTTPException(status_code=400, detail="ต้องระบุ line_user_id")
    # หมายเหตุ: ไม่บังคับ token ที่นี่ — endpoint นี้ใช้ "สร้าง session" ตอน LIFF auto-login
    #   (ตอนนั้นยังไม่มี token) การบังคับจะทำให้ login LINE พัง (chicken-and-egg)
    #   line_user_id เป็น id ทึบ (เดา/ไล่ไม่ได้) เหลือเป็น residual เล็กใน roadmap
    sb = get_supabase()
    res = sb.table("customers").select(_CUSTOMER_PUBLIC_FIELDS).eq("line_user_id", lid).limit(1).execute()
    return {"customer": (res.data[0] if res.data else None)}

def _display_name_taken(sb, name: str, exclude_lid: str = "", exclude_phone: str = "") -> bool:
    """ชื่อนี้มีคนอื่นใช้แล้วมั้ย (case-insensitive, ยกเว้นตัวเอง)
    ยกเว้นตัวเองได้ทั้งด้วย line_user_id (ลูกค้า LINE) และเบอร์โทร (ลูกค้า phone-only)
    ป้องกัน wildcard: แปลง %/* (และคง _) เป็น single-char wildcard เพื่อไม่ให้ ilike under-match
    แล้วยืนยัน exact ใน python อีกชั้น กันชื่อที่มี % _ * ไปแมตช์คนอื่นมั่ว"""
    nm = (name or "").strip()
    if not nm:
        return False
    like = nm.replace("%", "_").replace("*", "_")   # ทุก wildcard → '_' (แมตช์อักขระเดียวรวมถึงตัวจริง)
    try:
        rows = sb.table("customers").select("name,line_user_id,phone").ilike("name", like).execute().data or []
    except Exception as e:
        print(f"[name-check] error: {e}")
        return False
    tgt = nm.lower()
    exclude    = (exclude_lid or "").strip()
    exclude_ph = (exclude_phone or "").strip()
    def _is_self(r):
        if exclude and (r.get("line_user_id") or "").strip() == exclude:
            return True
        if exclude_ph and (r.get("phone") or "").strip() == exclude_ph:
            return True
        return False
    return any((r.get("name") or "").strip().lower() == tgt and not _is_self(r)
               for r in rows)

@app.get("/customers/check-name")
async def customer_check_name(name: str, line_user_id: str = "", phone: str = "", x_auth_token: str = Header(default="")):
    """เช็คชื่อซ้ำ (ยกเว้นตัวเอง) — ตอนแก้โปรไฟล์
    ยกเว้นตัวเองได้ทั้งด้วย line_user_id หรือเบอร์ (ลูกค้า phone-only)"""
    if not (name or "").strip():
        return {"taken": False}
    sb = get_supabase()
    return {"taken": _display_name_taken(sb, name, line_user_id, _norm_phone(phone or ""))}

class CustomerProfileBody(BaseModel):
    line_user_id:   Optional[str] = None   # optional — ลูกค้า phone-only (login ด้วย OTP) ไม่มี LINE
    display_name:   Optional[str] = None
    picture_url:    Optional[str] = None
    phone:          Optional[str] = None
    name:           Optional[str] = None
    address:        Optional[str] = None
    province:       Optional[str] = None
    zip:            Optional[str] = None
    notify_channel: Optional[str] = None

@app.post("/customers/profile")
async def upsert_customer_profile(body: CustomerProfileBody, x_auth_token: str = Header(default="")):
    """สร้าง/อัปเดตโปรไฟล์ลูกค้าด้วย LINE user id (แทน upsertCustomer เดิม)
    เช็คชื่อซ้ำก่อนตั้ง name — ทำงานด้วย service key ฝั่ง server
    คืน token ใหม่ (ผูกเบอร์ล่าสุด) ให้ frontend เก็บ — เผื่อลูกค้า LINE เพิ่งเพิ่มเบอร์"""
    lid = (body.line_user_id or "").strip()
    ph  = _norm_phone(body.phone or "")
    # ต้องระบุอย่างน้อยตัวใดตัวหนึ่งเพื่อชี้ตัวลูกค้า
    #  - ลูกค้า LINE: มี line_user_id → path เดิม ไม่บังคับ token (ใช้ตอน LIFF auto-login ที่ยังไม่มี token)
    #  - ลูกค้า phone-only (login ด้วย OTP ไม่มี LINE): ใช้เบอร์ชี้ตัว + ต้องพิสูจน์ token ว่าเป็นเจ้าของเบอร์
    if not lid and not ph:
        raise HTTPException(status_code=400, detail="ต้องระบุ line_user_id หรือ เบอร์โทร")
    if not lid:
        _check_customer(x_auth_token, phone=ph)   # บังคับ token เมื่อแก้ด้วยเบอร์ กันคนอื่นแก้โปรไฟล์เรา
    sb = get_supabase()
    nm = (body.name or "").strip()
    if nm and _display_name_taken(sb, nm, exclude_lid=lid, exclude_phone=ph):
        raise HTTPException(status_code=409, detail="ชื่อนี้มีคนใช้แล้ว กรุณาตั้งชื่ออื่นครับ")
    data = {"updated_at": datetime.utcnow().isoformat()}
    for k in ("display_name", "picture_url", "phone", "name", "address", "province", "zip", "notify_channel"):
        v = getattr(body, k)
        if v is None:
            continue
        # อย่าเขียน phone="" ลง DB — จะชน unique constraint customers_phone_unique
        # (มีหลาย row ที่ phone ว่าง; เขียน "" ซ้ำ = duplicate key → 500) ลูกค้า LINE ที่ยังไม่มีเบอร์เป็นเคสนี้
        if k == "phone" and str(v).strip() == "":
            continue
        data[k] = v
    if lid:
        # ลูกค้า LINE — upsert ด้วย line_user_id (conflict key) เหมือนเดิม
        data["line_user_id"] = lid
        res = sb.table("customers").upsert(data, on_conflict="line_user_id").execute()
    else:
        # ลูกค้า phone-only — update row ที่ตรงเบอร์ (guest auto-capture สร้าง row ไว้แล้ว)
        # ไม่ใช้ upsert เพราะ conflict key ของตารางคือ line_user_id ซึ่งคนกลุ่มนี้เป็น null
        res = sb.table("customers").update(data).eq("phone", ph).execute()
        if not res.data:   # เผื่อไม่มี row เดิม → insert ใหม่
            res = sb.table("customers").insert({**data, "phone": ph}).execute()
    cust = res.data[0] if res.data else None
    token = _make_customer_token(phone=(cust or {}).get("phone") or ph, line_user_id=lid)
    return {"customer": cust, "token": token}


# ============================================================
#  Admin reads (ผ่าน admin token) — แทนการยิง Supabase ตรงในหน้า admin
# ============================================================

_ADMIN_ORDER_FIELDS = ("order_id,order_date,ship_date,customer,phone,province,zip,full_address,"
                       "sku,qty,channel,status,slip_url,paid_at,note,total,preferred_carrier")

@app.get("/admin/orders-list")
async def admin_orders_list(sort: str = "created_at", limit: int = 1000,
                            ids: str = "", x_api_key: str = Header(default="")):
    """รายการออเดอร์ทั้งหมด (+ เลขพัสดุ) สำหรับหน้า admin/orders และหน้าพิมพ์ฉลาก
    - ถ้าส่ง ids=a,b,c → คืนเฉพาะออเดอร์เหล่านั้น"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    sort_col = sort if sort in ("created_at", "order_date") else "created_at"
    query = sb.table("orders").select(_ADMIN_ORDER_FIELDS)
    id_list = [i.strip() for i in (ids or "").split(",") if i.strip()]
    if id_list:
        query = query.in_("order_id", id_list)
    res = query.order(sort_col, desc=True).limit(min(int(limit or 1000), 2000)).execute()
    orders = res.data or []
    _join_tracking(sb, orders)
    return {"orders": orders, "count": len(orders)}

@app.get("/admin/order-by-tracking")
async def admin_order_by_tracking(tracking: str, x_api_key: str = Header(default="")):
    """หาลูกค้า/ออเดอร์จากเลขพัสดุ — โมดัลในหน้า admin สถานะพัสดุ"""
    check_admin_key(x_api_key)
    tk = (tracking or "").strip()
    if not tk:
        raise HTTPException(status_code=400, detail="ต้องระบุ tracking")
    sb = get_supabase()
    sh = sb.table("shipping").select("order_id").eq("tracking", tk).limit(1).execute()
    if not sh.data:
        return {"order": None}
    oid = sh.data[0].get("order_id")
    res = sb.table("orders").select(_ADMIN_ORDER_FIELDS).eq("order_id", oid).limit(1).execute()
    order = res.data[0] if res.data else None
    if order:
        order["tracking"] = tk
    return {"order": order}


@app.post("/admin/products/{product_id}")
async def update_product(product_id: int, body: dict, x_api_key: str = Header(default="")):
    """Admin — อัปเดตราคา/รายละเอียดสินค้า"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    allowed = {"name", "description", "flavor", "roast", "process", "price", "discount_pct", "image_url", "active", "in_stock", "sort_order"}
    update_data = {k: v for k, v in body.items() if k in allowed}
    if not update_data:
        raise HTTPException(status_code=400, detail="no valid fields")
    sb.table("products").update(update_data).eq("id", product_id).execute()
    return {"success": True}


# ── จัดการลูกค้า (Admin) ──────────────────────────────────────────
@app.get("/admin/customers")
async def list_customers(q: str = "", limit: int = 200, x_api_key: str = Header(default="")):
    """รายชื่อลูกค้า + ค้นหา (เบอร์/ชื่อ) สำหรับหน้าจัดการลูกค้า"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    sel = "id,phone,name,display_name,line_user_id,notify_channel,vip_discount_pct,first_order_used,created_at"
    query = sb.table("customers").select(sel)
    q = (q or "").strip()
    if q:
        safe = q.replace(",", " ").replace("(", "").replace(")", "").replace("*", "")
        query = query.or_(f"phone.ilike.*{safe}*,name.ilike.*{safe}*,display_name.ilike.*{safe}*")
    res = query.order("created_at", desc=True).limit(min(int(limit or 200), 500)).execute()
    rows = res.data or []
    for r in rows:
        r["has_line"] = bool(r.get("line_user_id"))
        r.pop("line_user_id", None)   # ไม่ต้องส่ง id ดิบออกไปหน้าเว็บ
        r["vip_discount_pct"] = int(r.get("vip_discount_pct") or 0)
    return {"customers": rows, "count": len(rows)}


class VipUpdateRequest(BaseModel):
    id:               Optional[int] = None
    phone:            Optional[str] = None
    vip_discount_pct: int = 0

@app.post("/admin/customers/vip")
async def set_customer_vip(body: VipUpdateRequest, x_api_key: str = Header(default="")):
    """ตั้ง/แก้ % ส่วนลด VIP ของลูกค้าหนึ่งคน (0-100, 0 = ยกเลิก VIP)"""
    check_admin_key(x_api_key)
    pct = int(body.vip_discount_pct or 0)
    if pct < 0 or pct > 100:
        raise HTTPException(status_code=400, detail="vip_discount_pct ต้องอยู่ระหว่าง 0-100")
    sb = get_supabase()
    if body.id is not None:
        col, val = "id", body.id
    elif body.phone:
        col, val = "phone", body.phone.strip()
    else:
        raise HTTPException(status_code=400, detail="ต้องระบุ id หรือ phone")
    res = sb.table("customers").update({"vip_discount_pct": pct}).eq(col, val).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="ไม่พบลูกค้า")
    return {"success": True, "vip_discount_pct": pct}


def _barcodes_in_system(sb, barcodes):
    """คืน set ของเลขพัสดุที่ 'มีจริงในระบบร้าน' (shipments.barcode หรือ shipping.tracking)
    คืน None ถ้า query DB error → ให้ caller fail-open (ไม่บล็อกลูกค้าจริงตอน DB สะอึก)"""
    if not barcodes:
        return set()
    found = set()
    try:
        r1 = sb.table("shipments").select("barcode").in_("barcode", barcodes).execute()
        for row in (r1.data or []):
            if row.get("barcode"):
                found.add(row["barcode"].upper())
        r2 = sb.table("shipping").select("tracking").in_("tracking", barcodes).execute()
        for row in (r2.data or []):
            if row.get("tracking"):
                found.add(row["tracking"].upper())
    except Exception as e:
        print(f"[track] เช็ค DB error: {e}")
        return None
    return found


@app.get("/track/{barcode}")
async def track_single(barcode: str):
    """เช็คสถานะพัสดุ 1 ชิ้น (real-time) — เฉพาะเลขที่มีในระบบร้าน (กันเช็คเลขคนอื่น/มั่ว)"""
    bc = barcode.upper().strip()
    if not bc:
        raise HTTPException(status_code=400, detail="กรุณาระบุเลขพัสดุ")
    sb = get_supabase()
    found = _barcodes_in_system(sb, [bc])
    if found is not None and bc not in found:
        return {"barcode": bc, "status": "not_found", "status_th": "ไม่พบเลขพัสดุในระบบร้าน"}
    return await fetch_tracking(bc)


@app.post("/track/bulk")
async def track_bulk(body: BulkRequest):
    """เช็คสถานะพัสดุหลายชิ้นพร้อมกัน (real-time, สูงสุด 20)
    เช็คทุกเลขว่าอยู่ในระบบร้านจริงก่อนเสมอ — กันคนใช้เราเป็นตัวเช็คพัสดุฟรี/เลขมั่ว
    """
    barcodes = [b.upper().strip() for b in body.barcodes if b.strip()]
    if not barcodes:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcodes")
    if len(barcodes) > 20:
        raise HTTPException(status_code=400, detail="ส่งได้สูงสุด 20 เลขต่อครั้ง")

    # เช็คทุกเลข (รวม Flash/Kerry/ไปรษณีย์) ว่าอยู่ในระบบร้านไหม
    sb = get_supabase()
    found = _barcodes_in_system(sb, barcodes)
    valid_barcodes = set(barcodes) if found is None else found  # DB error → fail-open

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
    # ถ้าเป็น datetime/date/Timestamp อยู่แล้ว → format ตรง ไม่ต้องเดา
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    s = str(v).strip()
    # รูปแบบ ISO 'YYYY-MM-DD' (อาจมีเวลาต่อท้าย) — ชัดเจน parse ตรง ห้ามใช้ dayfirst
    # (dayfirst=True เจอ ISO จะสลับเดือน/วัน เช่น 2026-08-03 -> 2026-03-08)
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # fallback: รูปแบบเดิม DD/MM/YYYY (ไทย) — ใช้ dayfirst=True
    try:
        ts = pd.to_datetime(s, dayfirst=True)
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
import hmac, hashlib, base64

ADMIN_API_KEY  = os.getenv("ADMIN_API_KEY", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
# secret สำหรับเซ็น session token — ใช้ ADMIN_SECRET ถ้ามี ไม่งั้น fallback เป็น ADMIN_API_KEY
_ADMIN_SECRET  = (os.getenv("ADMIN_SECRET") or ADMIN_API_KEY or "").encode()
ADMIN_TOKEN_TTL = int(os.getenv("ADMIN_TOKEN_TTL", str(8 * 3600)))  # อายุ token (วินาที)

def _hmac_eq(a: str, b: str) -> bool:
    """เทียบสตริงแบบ constant-time ที่ทนอักขระ non-ASCII
    (hmac.compare_digest กับ str จะ throw TypeError ถ้ามีอักขระ >127 เช่นพิมพ์ไทยติดมา)
    encode เป็น bytes ก่อนเทียบ → ผลลัพธ์เป็น "ไม่ตรง" แทนที่จะพัง 500"""
    try:
        return hmac.compare_digest((a or "").encode("utf-8"), (b or "").encode("utf-8"))
    except Exception:
        return False

def _make_admin_token(exp: int) -> str:
    """สร้าง signed session token: v1.<exp>.<hmac>  (stateless ไม่ต้องเก็บ DB)"""
    msg = f"v1.{exp}".encode()
    sig = hmac.new(_ADMIN_SECRET, msg, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")
    return f"v1.{exp}.{sig_b64}"

def _verify_admin_token(tok: str) -> bool:
    """ตรวจ session token: รูปแบบถูก + ยังไม่หมดอายุ + ลายเซ็นตรง"""
    if not tok or not _ADMIN_SECRET:
        return False
    parts = tok.split(".")
    if len(parts) != 3 or parts[0] != "v1":
        return False
    try:
        exp = int(parts[1])
    except ValueError:
        return False
    if exp < int(time.time()):
        return False
    expected = _make_admin_token(exp)
    return _hmac_eq(tok, expected)

def check_admin_key(request_key: str):
    """ตรวจ admin credential — รับได้ทั้ง signed session token (จากหน้าเว็บ)
    หรือ static ADMIN_API_KEY (server-to-server). fail-closed."""
    if request_key and _verify_admin_token(request_key):
        return
    if ADMIN_API_KEY and _hmac_eq(request_key or "", ADMIN_API_KEY):
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


class AdminLoginBody(BaseModel):
    password: str

@app.post("/admin/login")
def admin_login(body: AdminLoginBody):
    """ล็อกอิน admin — ตรวจรหัสผ่านฝั่ง server แล้วออก session token (ไม่ส่ง key จริงไป browser)"""
    if not ADMIN_PASSWORD:
        # ยังไม่ตั้งรหัส — กันไม่ให้ล็อกอินได้เลย (fail-closed)
        raise HTTPException(status_code=503, detail="ยังไม่ได้ตั้งค่า ADMIN_PASSWORD บนเซิร์ฟเวอร์")
    if not _ADMIN_SECRET:
        raise HTTPException(status_code=503, detail="ยังไม่ได้ตั้งค่า ADMIN_SECRET/ADMIN_API_KEY บนเซิร์ฟเวอร์")
    if not _hmac_eq(body.password, ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="รหัสผ่านไม่ถูกต้อง")
    exp = int(time.time()) + ADMIN_TOKEN_TTL
    return {"token": _make_admin_token(exp), "expires_at": exp}


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

    # กัน FK error (23503): shipping.order_id ต้องมีอยู่จริงในตาราง orders
    # order_id ที่ valid = ออเดอร์ที่เพิ่ง import รอบนี้ + ที่มีอยู่แล้วใน DB
    valid_oids = {row["order_id"] for row in order_rows if row.get("order_id")}
    ship_oids  = list({r["order_id"] for r in shipping_rows})
    missing    = [o for o in ship_oids if o not in valid_oids]
    for i in range(0, len(missing), 100):
        chunk = missing[i:i+100]
        try:
            res = sb.table("orders").select("order_id").in_("order_id", chunk).execute()
            for row in (res.data or []):
                valid_oids.add(row["order_id"])
        except Exception as e:
            print(f"[shipping] เช็ค order_id ใน DB ไม่ได้: {e}")
    skipped_ship  = [r["order_id"] for r in shipping_rows if r["order_id"] not in valid_oids]
    shipping_rows = [r for r in shipping_rows if r["order_id"] in valid_oids]
    if skipped_ship:
        stats["shipping_skipped"] = skipped_ship
        print(f"[shipping] ข้าม {len(skipped_ship)} แถว — order_id ไม่มีในตาราง orders: {skipped_ship}")

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

    # กันสั่งสินค้าที่ปิดขาย (active=False) หรือหมด (in_stock=False) — เผื่อ cart ค้างจากตอนยังมีของ
    try:
        _ps = sb.table("products").select("sku,active,in_stock").execute()
        _pmap = {(p.get("sku") or "").upper(): p for p in (_ps.data or [])}
        _bad = []
        for i in body.items:
            pr = _pmap.get((i.sku or "").upper())
            if pr is not None and (pr.get("active") is False or pr.get("in_stock") is False):
                _bad.append(i.name or i.sku)
        if _bad:
            raise HTTPException(status_code=409, detail=f"สินค้าหมดหรือปิดขาย: {', '.join(_bad)} — กรุณานำออกจากตะกร้า")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[orders/create] stock check error: {e}")

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
    # นโยบายใหม่ (27/07): guest ได้ส่วนลดด้วย — ผูกกับ "เบอร์ที่กรอกตอน checkout" ไม่บังคับ login
    # กันใช้สิทธิ์ซ้ำด้วย _is_first_order_eligible(เบอร์) เหมือนเดิม (เช็ค first_order_used + ไม่เคยมีออเดอร์เว็บ)
    eligible   = (
        bool(body.first_order_discount)
        and bool(elig_phone)                       # ขอแค่มีเบอร์ (login หรือ guest ก็ได้)
        and _promo_first_order_enabled()
        and _is_first_order_eligible(sb, elig_phone)
    )
    fo_disc  = _first_order_discount(subtotal) if eligible else 0
    # ส่วนลด VIP — อัตโนมัติถ้าเบอร์นี้เป็นลูกค้า VIP (ผูก line_user_id หรือเบอร์)
    # คิดจาก "ราคาตั้ง" (original) ไม่ใช่ราคาที่ลด 30% แล้ว — VIP% แทนที่ส่วนลด 30% ปกติ (ไม่ลดซ้ำ)
    vip_pct  = _get_vip_pct(sb, elig_phone, body.line_user_id or "")
    vip_disc = 0
    if vip_pct > 0:
        orig_price = {}
        load_ok = True
        try:
            _p = sb.table("products").select("sku,price").execute()
            for p in (_p.data or []):
                orig_price[(p.get("sku") or "").upper()] = float(p.get("price") or 0)
        except Exception as e:
            load_ok = False
            print(f"[vip] load original prices error: {e}")
        # ต้องหา "ราคาตั้ง" ได้ครบทุกชิ้นก่อน ถึงจะคิด VIP
        # ถ้าโหลดไม่สำเร็จ หรือมี SKU ไหนหาไม่เจอ → ข้าม VIP รอบนี้ (กันเก็บเงินขาดจากการ
        #  fallback ไปใช้ราคาที่ลด 30% แล้วเป็นฐาน ซึ่งจะกลายเป็นลดซ้ำ)
        all_resolved = load_ok and all((i.sku or "").upper() in orig_price for i in body.items)
        if all_resolved:
            orig_subtotal = sum(orig_price[(i.sku or "").upper()] * i.qty for i in body.items)
            vip_total = int(round(orig_subtotal * (1 - vip_pct / 100.0)))
            vip_disc  = max(0, int(round(subtotal)) - vip_total)   # ส่วนลดเทียบกับราคาปกติหน้าเว็บ
        else:
            vip_disc = 0
            print(f"[vip] ข้ามส่วนลด VIP order {body.order_id} — หาราคาตั้งไม่ครบ (load_ok={load_ok})")
    # เลือกอันที่ลดเยอะกว่า (ไม่ซ้อน) — ตามที่ตกลง
    discount    = max(fo_disc, vip_disc)
    used_first  = fo_disc > 0 and fo_disc >= vip_disc   # โปรลูกค้าใหม่เป็นตัวที่ถูกใช้จริง
    total_paid  = subtotal - discount   # ยอดที่ลูกค้าจ่ายจริง (Pixel Purchase ใช้ค่านี้)

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
        "first_order_discount": used_first,   # True เฉพาะเมื่อโปรลูกค้าใหม่เป็นตัวที่ถูกใช้ (VIP ล้วนไม่กินสิทธิ์ครั้งแรก)
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
    # เฉพาะตอนที่ "โปรลูกค้าใหม่ 50%" ถูกใช้จริง (used_first) — ออเดอร์ VIP ล้วนไม่กินสิทธิ์
    # (กันเคส VIP สั่งแล้วไม่จ่าย → cron ลบ แต่ไม่คืนสิทธิ์ 50% ที่ไม่เคยได้ใช้)
    if used_first:
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
        f"ยอด: ฿{total_paid:,.0f}" + (f" ({'ลดลูกค้าใหม่' if used_first else f'VIP {vip_pct}%'} -฿{discount:,.0f})" if discount > 0 else "") + "\n"
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
        channel = _resolve_notify_channel(notify_ch, line_uid)
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


# กันยิงซ้ำ /slip-notify (double-submit) — Render รัน WEB_CONCURRENCY=1 ใช้ set ในหน่วยความจำได้
_slip_inflight: set = set()

@app.post("/orders/slip-notify")
async def slip_notify(body: SlipNotifyRequest):
    """ตรวจสอบสลิปผ่าน SlipOK → auto-confirm ถ้ายอดตรง หรือแจ้ง admin"""
    sb = get_supabase()
    res = sb.table("orders").select(
        "customer,phone,total,status,first_order_discount,slip_url,slip_status"
    ).eq("order_id", body.order_id).execute()
    order = res.data[0] if res.data else {}
    customer = order.get("customer", "ลูกค้า")
    phone    = order.get("phone", "")
    total    = float(order.get("total") or 0)

    if order.get("status") in ("ชำระแล้ว", "เตรียมจัดส่ง", "จัดส่งแล้ว", "จัดส่งสำเร็จ"):
        return {"success": True, "verified": False, "reason": "already_paid"}

    # กันยิงซ้ำ (1): สลิปใบเดิม (URL เดิม) ที่ตรวจไปแล้ว → คืนผลเดิม ไม่เรียก SlipOK ซ้ำ
    # (ถ้าตรวจสลิปเดิมซ้ำ SlipOK จะขึ้น 1010 "ซ้ำ" ทั้งที่ลูกค้าโอนจริง)
    if order.get("slip_url") and order.get("slip_url") == body.slip_url and order.get("slip_status"):
        return {"success": True, "verified": order.get("slip_status") == "verified", "reason": "already_checked"}

    # กันยิงซ้ำ (2): คำขอซ้อนของ order เดียวกัน (double-submit พร้อมกัน)
    if body.order_id in _slip_inflight:
        return {"success": True, "verified": False, "reason": "processing"}
    _slip_inflight.add(body.order_id)

    try:
        slip_verified = False
        slip_amount   = None
        slip_ref      = None
        slip_error    = None
        slip_soft     = None   # ปัญหาชั่วคราว (เช่น ธนาคารต้องรอ) — ไม่ตีเป็น rejected ให้ลองใหม่ได้

        if SLIPOK_API_KEY:
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    # ไม่ส่ง amount ตายตัว → กัน 1013 จากยอดลด 50%/ปัดเศษไม่ตรงเป๊ะ
                    # SlipOK ยังเช็คให้ว่าเป็นสลิปจริง + เข้าบัญชีร้าน (1014) + ไม่ซ้ำ (1012)
                    # 1010 = ธนาคารกำลังตรวจ (ยังไม่พร้อม) → ลองซ้ำเองอัตโนมัติสูงสุด 3 ครั้ง (หน่วง 8 วิ)
                    #        ลูกค้าไม่ต้องแนบสลิปใหม่ — ส่วนใหญ่ 1010 เคลียร์ในไม่กี่วินาที
                    for _attempt in range(3):
                        r = await client.post(
                            SLIPOK_API_URL,
                            headers={"x-authorization": SLIPOK_API_KEY},
                            data={"url": body.slip_url, "log": "true"},
                        )
                        rdata = r.json()
                        if rdata.get("success"):
                            slip_amount   = float(rdata.get("data", {}).get("amount") or 0)
                            slip_ref      = rdata.get("data", {}).get("transRef", "")
                            slip_verified = True   # สลิปจริง + เข้าบัญชีร้าน + ไม่ซ้ำ → ยืนยันเลย (เตือน admin ถ้ายอดต่าง)
                            break
                        err_code = rdata.get("code", 0)
                        if err_code == 1010 and _attempt < 2:
                            print(f"[SlipOK] {body.order_id} 1010 (ธนาคารกำลังตรวจ) → ลองใหม่อัตโนมัติ #{_attempt+1} ใน 8 วิ")
                            await asyncio.sleep(8)
                            continue
                        # error อื่น หรือ 1010 หลังลองครบแล้ว → จบการวน
                        # โค้ด SlipOK จริง: 1012=สลิปซ้ำ, 1013=ยอดไม่ตรง, 1014=บัญชีผู้รับไม่ตรงบัญชีหลักร้าน, 1010=ธนาคารกำลังตรวจ
                        if err_code == 1014:   slip_error = "สลิปนี้ไม่ได้โอนเข้าบัญชีหลักของร้าน"
                        elif err_code == 1012: slip_error = "สลิปซ้ำ (เคยส่งเข้ามาแล้ว) — โปรดเช็คยอดเข้าบัญชีก่อนยืนยัน"
                        elif err_code == 1010:
                            slip_error = None   # ไม่ตีเป็น rejected
                            slip_soft  = "ธนาคารนี้ต้องรอตรวจสอบสักครู่ กรุณาลองแนบสลิปใหม่อีกครั้งใน 1-2 นาที"
                        else:
                            _msg = (rdata.get("message") or "").strip()
                            slip_error = _msg or f"อ่านสลิปอัตโนมัติไม่ได้ (code={err_code}) — โปรดตรวจเอง"
                            if not _msg:   # เคส err=- ที่ไม่มีเหตุผล → log response ดิบไว้ดูว่า SlipOK ตอบอะไรจริง
                                print(f"[SlipOK] {body.order_id} raw={str(rdata)[:400]}")
                        break
                    print(f"[SlipOK] {body.order_id} → {'✓ verified' if slip_verified else '✗ manual'} "
                          f"slip=฿{(slip_amount or 0):.0f} order=฿{total:.0f} ref={slip_ref or '-'} err={slip_error or '-'}")
            except Exception as e:
                slip_error = f"SlipOK error: {e}"
                print(f"[SlipOK] {body.order_id} error: {e}")
        else:
            print(f"[SlipOK] {body.order_id} skip (ไม่มี SLIPOK_API_KEY) → manual")

        # กันสลิปยอดน้อยกว่าออเดอร์ชัดเจน (เช่นโอน ฿20 ให้ออเดอร์ ฿500) — ไม่ auto-confirm
        # เช็คเฉพาะตอน SlipOK อ่านยอดได้จริง (slip_amount>0) และน้อยกว่าเกิน ฿2 → ให้ admin ตรวจแทน
        # (ไม่ได้ส่งยอดไป SlipOK เลยไม่กระทบ 1013 จากยอดลด/ปัดเศษ; ยอดเกินยังผ่านปกติ)
        if slip_verified and slip_amount and total and slip_amount < total - 2:
            slip_verified = False
            slip_error = f"ยอดสลิป ฿{slip_amount:,.0f} น้อยกว่ายอดออเดอร์ ฿{total:,.0f} — โปรดตรวจก่อนยืนยัน"
            print(f"[SlipOK] {body.order_id} ยอดน้อยกว่าออเดอร์ → ส่งให้ admin ตรวจ")

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
            warn = ""
            if slip_amount and total and abs(slip_amount - total) > 2:
                warn = f"\n⚠️ ยอดสลิป ฿{slip_amount:,.0f} ≠ ยอด order ฿{total:,.0f} — โปรดตรวจ"
            await send_line_notify(ADMIN_LINE_USER_ID,
                f"✅ Auto-confirm! {customer} ({phone})\nOrder: {body.order_id}\nยอด: ฿{slip_amount:,.0f}\nRef: {slip_ref}{warn}")
            # แจ้งลูกค้าทาง LINE ว่ายืนยันการชำระเงินแล้ว
            await _notify_customer(sb, body.order_id, phone, customer,
                f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{body.order_id} เรียบร้อยแล้วค่ะ ✅\n"
                f"กำลังแพ็คของและจัดส่งให้เร็วที่สุด เดี๋ยวมีเลขพัสดุแจ้งอีกทีนะคะ 🐰\n"
                f"ดูสถานะ: velacoldbrew.com/account",
                f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{body.order_id} แล้วค่ะ กำลังแพ็คและจัดส่ง เดี๋ยวแจ้งเลขพัสดุอีกทีนะคะ ดูสถานะ velacoldbrew.com/account",
                "payment_confirmed")
            return {"success": True, "verified": True, "amount": slip_amount, "ref": slip_ref}
        else:
            # ISSUE 2 (27/07): ไม่ใช้คำว่า "rejected" อีก — สื่อว่าลูกค้าโกง ทั้งที่ส่วนใหญ่จ่ายจริง
            # ตรวจไม่ผ่าน/SlipOK ล่ม/quota → "pending_review" (รอ admin ตรวจ) ไม่ใช่ปฏิเสธ
            sb.table("orders").update({
                "slip_url":    body.slip_url,
                "slip_status": "pending_review" if slip_error else "pending",
            }).eq("order_id", body.order_id).execute()
            # 1010 (ธนาคารต้องรอ) → บอกลูกค้าให้ลองใหม่ + แจ้ง admin ด้วย (กันออเดอร์หลุดถ้าลูกค้าไม่แนบซ้ำ)
            if slip_soft and not slip_error:
                try:
                    await send_line_notify(ADMIN_LINE_USER_ID,
                        f"⏳ สลิปรอธนาคารตรวจ (SlipOK 1010) — ลูกค้าจ่ายแล้วแต่ auto-confirm ไม่ได้\n"
                        f"ชื่อ: {customer}" + (f" ({phone})" if phone else "") +
                        f"\nOrder: {body.order_id}" + (f"\nยอด order: ฿{total:,.0f}" if total else "") +
                        f"\nลูกค้าได้รับข้อความให้แนบสลิปใหม่ใน 1-2 นาที — ถ้าไม่แนบซ้ำ โปรดเช็คยอดเข้าบัญชีแล้วยืนยันใน /admin/orders")
                except Exception as e:
                    print(f"[SlipOK] 1010 admin-notify error: {e}")
                return {"success": True, "verified": False, "retry": True, "reason": slip_soft}
            msg = (f"💳 ลูกค้าส่งสลิปแล้ว!\nชื่อ: {customer}" +
                   (f" ({phone})" if phone else "") +
                   f"\nOrder: {body.order_id}" +
                   (f"\nยอด order: ฿{total:,.0f}" if total else "") +
                   (f"\n⚠️ {slip_error}" if slip_error else "") +
                   f"\nกรุณายืนยันชำระเงินใน /admin/orders")
            await send_line_notify(ADMIN_LINE_USER_ID, msg)
            return {"success": True, "verified": False, "reason": slip_error or "manual_check"}
    finally:
        _slip_inflight.discard(body.order_id)


class PaymentSmsRequest(BaseModel):
    order_id: str
    message: Optional[str] = None

@app.post("/admin/send-payment-sms")
async def send_payment_sms(body: PaymentSmsRequest, x_api_key: str = Header(default="")):
    """ส่ง SMS แจ้งลูกค้าให้ชำระเงิน (แอดมินกดจากหน้า Orders) — ส่งซ้ำได้"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    res = sb.table("orders").select("customer,phone,total,status").eq("order_id", body.order_id).execute()
    order = res.data[0] if res.data else None
    if not order:
        raise HTTPException(status_code=404, detail="ไม่พบออเดอร์")
    phone = (order.get("phone") or "").strip()
    if not phone or phone == "-" or len(phone) < 9:
        raise HTTPException(status_code=400, detail="ออเดอร์นี้ไม่มีเบอร์โทรที่ส่ง SMS ได้")
    customer = order.get("customer") or "ลูกค้า"
    total = int(float(order.get("total") or 0))
    msg = body.message or (
        f"VeLA Cold Brew: กรุณาชำระออเดอร์ #{body.order_id} ยอด {total} บาท "
        f"ที่ velacoldbrew.com เมนูบัญชีของฉัน (เข้าสู่ระบบด้วยเบอร์นี้) ขอบคุณค่ะ"
    )
    ok = await send_sms(phone, msg, barcode=body.order_id, status="payment_reminder", customer=customer, force=True)
    if not ok:
        raise HTTPException(status_code=502, detail="ส่ง SMS ไม่สำเร็จ (เช็คเครดิต/คีย์ SMS)")
    return {"success": True, "phone": phone}


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
    from_sku = order.get("sku", "")
    phone, customer = _loyalty_identity(sb, order)  # ผูกแต้มเข้าบัญชีคนสั่ง (login)
    # คำนวณ point จาก SKU (100ml = 1 point) — ใช้ parser ตัวเดียวกับ confirm-payment
    # (เดิมเช็คแค่ '1L'/'1000' ในชื่อ ทำให้ sku เว็บ 'Dark x1' นับเป็น 0 → ไม่ได้แต้ม)
    total_ml = parse_shopee_sku_ml(from_sku)
    points = total_ml / 100
    if points > 0 and phone:
        try:
            sb.table("point_ledger").upsert({
                "order_id":   order_id,
                "phone":      phone,
                "customer":   customer,
                "channel":    order.get("channel", "web"),
                "ml_total":   total_ml,
                "points":     points,
                "order_date": order.get("order_date", datetime.utcnow().strftime("%Y-%m-%d")),
            }, on_conflict="order_id").execute()
        except Exception as e:
            print(f"[point_ledger] {order_id}: {e}")


@app.post("/admin/backfill-points")
async def backfill_points(x_api_key: str = Header(default="")):
    """คำนวณแต้มย้อนหลังให้ออเดอร์เว็บที่ชำระแล้วทุกใบ (idempotent — รันซ้ำได้ ไม่ซ้ำแต้ม)
    แก้เคสที่ auto-verify แล้วไม่ได้แต้มเพราะบั๊ก _award_points เดิม"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    PAID = ["ชำระแล้ว", "เตรียมจัดส่ง", "จัดส่งแล้ว", "จัดส่งสำเร็จ"]
    res = sb.table("orders").select("order_id").eq("channel", "web").in_("status", PAID).execute()
    order_ids = [r["order_id"] for r in (res.data or [])]
    processed = 0
    for oid in order_ids:
        try:
            await _award_points(sb, oid)
            processed += 1
        except Exception as e:
            print(f"[backfill-points] {oid}: {e}")
    print(f"[backfill-points] เช็ค {len(order_ids)} ออเดอร์เว็บที่ชำระแล้ว")
    return {"success": True, "checked": len(order_ids), "processed": processed}


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

    # อัปเดตสถานะ order
    # ใส่เลขพัสดุจริง → "เตรียมจัดส่ง" (คีเลข/แปะ label แล้ว แต่ขนส่งยังไม่รับพัสดุ)
    #   cron จะเลื่อนเป็น "จัดส่งแล้ว" อัตโนมัติเมื่อขนส่ง scan รับพัสดุจริง (accepted/in_transit)
    # ออเดอร์ส่งเอง (ไม่มีเลข/"-") → ข้ามไป "จัดส่งแล้ว" เลย เพราะไม่มีเลขให้ cron ตามความเคลื่อนไหว
    self_delivery = (not trk) or trk == "-"
    new_status = "จัดส่งแล้ว" if self_delivery else "เตรียมจัดส่ง"
    sb.table("orders").update({
        "status":    new_status,
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

    # เพิ่มลง shipments (ระบบติดตาม) — เฉพาะพัสดุที่มีเลข tracking จริงเท่านั้น
    # ออเดอร์ส่งเอง (ไม่มีเลข/"-") ไม่ต้องเข้าระบบติดตาม ไม่งั้นจะค้างเป็น barcode ว่างให้ cron เช็คทุกวัน
    if not self_delivery:
        existing = sb.table("shipments").select("barcode").eq("barcode", trk).execute()
        if not existing.data:
            sb.table("shipments").insert({"barcode": trk, "status": "pending"}).execute()

    # หมายเหตุ (28/07): ไม่ยิงแจ้ง "จัดส่งแล้ว" ตรงนี้อีกต่อไป
    # เพราะตอนคีเลขเข้า ShipSmile ของยังไม่ออกจริง → ลูกค้าจะได้ SMS เร็วไป
    # ระบบจะแจ้ง "ร้านได้จัดกาแฟของคุณแล้ว 📦 + ลิงก์ติดตาม" อัตโนมัติจาก cron
    # ตอนขนส่ง scan รับพัสดุจริง (pending → in_transit → template 'accepted')
    return {"success": True, "order_id": body.order_id, "tracking": body.tracking.strip().upper()}


class FixTrackingRequest(BaseModel):
    order_id: str
    tracking: str                       # เลขพัสดุใหม่ (ที่ถูก)
    carrier:  Optional[str] = None      # ถ้าไม่ส่ง ใช้ business_carrier เดาจากเลข
    notify:   bool = True               # ยิงแจ้งเตือนเลขใหม่ให้ลูกค้า

@app.post("/admin/fix-tracking")
async def fix_tracking(body: FixTrackingRequest, x_api_key: str = Header(default="")):
    """แก้เลขพัสดุที่กรอกผิดของ order — อัปเดต shipping + shipments ให้ตรง แล้วยิงแจ้งเตือนเลขใหม่ให้ลูกค้า"""
    check_admin_key(x_api_key)
    sb = get_supabase()
    new_trk = body.tracking.strip().upper()
    if not new_trk or new_trk == "-":
        raise HTTPException(status_code=400, detail="ต้องระบุเลขพัสดุใหม่")
    carrier = business_carrier(new_trk, body.carrier or "POST SABUY")

    o = sb.table("orders").select("customer,phone,ship_date").eq("order_id", body.order_id).execute()
    if not o.data:
        raise HTTPException(status_code=404, detail=f"ไม่พบ order_id: {body.order_id}")
    order = o.data[0]

    # หา shipping row เดิมของ order นี้ (เพื่อรู้เลขเก่าไปแก้ shipments)
    sh = sb.table("shipping").select("tracking").eq("order_id", body.order_id).execute()
    old_trk = (sh.data[0].get("tracking") if sh.data else "") or ""
    old_trk_up = old_trk.strip().upper()

    if new_trk == old_trk_up:
        # เลขเดิมถูกอยู่แล้ว — แค่ยิงแจ้งเตือนซ้ำถ้าต้องการ
        pass

    # 1) shipping — อัปเดตเลข + carrier (อิง order_id เป็นหลัก)
    if sh.data:
        sb.table("shipping").update({"tracking": new_trk, "carrier": carrier}).eq("order_id", body.order_id).execute()
    else:
        sb.table("shipping").upsert({
            "order_id":  body.order_id,
            "ship_date": order.get("ship_date") or datetime.utcnow().strftime("%Y-%m-%d"),
            "carrier":   carrier,
            "tracking":  new_trk,
        }, on_conflict="tracking").execute()

    # 2) shipments (ระบบติดตาม) — ย้ายเลขเก่า→ใหม่ + reset สถานะให้ cron เช็คเลขใหม่ใหม่
    moved = False
    if old_trk_up and old_trk_up != "-" and old_trk_up != new_trk:
        try:
            r = sb.table("shipments").update({
                "barcode": new_trk, "status": "pending", "status_th": "รอข้อมูล",
                "is_done": False, "latest_location": None,
            }).eq("barcode", old_trk_up).execute()
            moved = bool(r.data)
        except Exception as e:
            print(f"[fix-tracking] shipments move error: {e}")
    if not moved:
        exists = sb.table("shipments").select("barcode").eq("barcode", new_trk).execute()
        if not exists.data:
            sb.table("shipments").insert({"barcode": new_trk, "status": "pending"}).execute()

    # 3) แจ้งเตือนเลขใหม่ให้ลูกค้า (LINE ถ้าผูก ไม่งั้น SMS)
    notified = None
    if body.notify:
        ship_msg = (f"VeLA Cold Brew: อัปเดตเลขพัสดุออเดอร์ #{body.order_id} นะคะ 🚚\n"
                    f"ขนส่ง: {carrier} · เลขพัสดุ: {new_trk}\n"
                    f"ติดตามพัสดุ: velacoldbrew.com/track/{new_trk}")
        sms_ship = (f"VeLA Cold Brew: อัปเดตเลขพัสดุออเดอร์ #{body.order_id} ขนส่ง {carrier} "
                    f"เลขพัสดุ {new_trk} ติดตาม velacoldbrew.com/track/{new_trk}")
        # ใช้ status_tag แยกจาก "shipped" — กันชน unique (barcode,status) ใน sms_logs ตอน resend
        notified = await _notify_customer(sb, body.order_id, order.get("phone") or "", order.get("customer") or "",
                                          ship_msg, sms_ship, "tracking_updated")

    return {"success": True, "order_id": body.order_id,
            "old_tracking": old_trk_up or None, "new_tracking": new_trk,
            "carrier": carrier, "notified_via": notified}


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
                # ลูกค้าที่ผูก LINE ไว้ → รับแจ้งเตือนทาง LINE เสมอ (เว้นตั้งค่าปิดชัดเจน)
                notify_ch = _resolve_notify_channel(cust.data[0].get("notify_channel"), line_uid)
        except:
            pass

        msg = "VeLA Cold Brew: พัสดุของคุณถึงแล้ว ✓ ขอบคุณที่สั่งซื้อนะคะ 🐰 สั่งซื้อและรับสิทธิพิเศษสมาชิกได้ที่: velacoldbrew.com"
        if notify_ch == "off":
            print(f"[confirm-delivered] ข้าม {customer} → ปิดแจ้งเตือน")
        elif notify_ch == "line" and line_uid:
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
    await _notify_customer(sb, order_id, phone or "", order.get("customer") or "",
        f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{order_id} เรียบร้อยแล้วค่ะ ✅\n"
        f"กำลังแพ็คของและจัดส่งให้เร็วที่สุด เดี๋ยวมีเลขพัสดุแจ้งอีกทีนะคะ 🐰\n"
        f"ดูสถานะ: velacoldbrew.com/account",
        f"VeLA Cold Brew: ยืนยันการชำระเงินออเดอร์ #{order_id} แล้วค่ะ กำลังแพ็คและจัดส่ง เดี๋ยวแจ้งเลขพัสดุอีกทีนะคะ ดูสถานะ velacoldbrew.com/account",
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
OTP_STORE: dict[str, dict] = {}  # phone -> {otp, expires, attempts}
OTP_REQ_LOG: dict[str, list] = {}  # phone -> [timestamps] สำหรับ rate limit (กัน SMS bomb)

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
    if len(phone) < 9 or not phone.isdigit():
        raise HTTPException(status_code=400, detail="เบอร์โทรไม่ถูกต้อง")

    import time
    now = time.time()
    # rate limit ต่อเบอร์ — กัน SMS bomb: เว้นอย่างน้อย 60 วิ/ครั้ง, ไม่เกิน 5 ครั้ง/ชม.
    log = [t for t in OTP_REQ_LOG.get(phone, []) if now - t < 3600]
    if log and now - log[-1] < 60:
        raise HTTPException(status_code=429, detail="ขอ OTP ถี่เกินไป รอสักครู่แล้วลองใหม่ค่ะ")
    if len(log) >= 5:
        raise HTTPException(status_code=429, detail="ขอ OTP เกินกำหนดต่อชั่วโมง กรุณาลองใหม่ภายหลัง")
    log.append(now)
    OTP_REQ_LOG[phone] = log

    # สร้าง OTP 6 หลัก
    otp = str(random.randint(100000, 999999))
    OTP_STORE[phone] = {"otp": otp, "expires": now + 300, "attempts": 0}  # หมดอายุ 5 นาที

    # ส่ง SMS
    msg = f"VeLA Cold Brew: รหัส OTP ของคุณคือ {otp} (หมดอายุใน 5 นาที)"
    success = await send_sms(phone, msg)

    if not success:
        raise HTTPException(status_code=500, detail="ส่ง OTP ไม่สำเร็จ")

    # บอก frontend ว่าเบอร์นี้เป็นสมาชิกใหม่ไหม (ใหม่ = บังคับกรอกชื่อ)
    try:
        _ex = get_supabase().table("customers").select("phone").eq("phone", phone).execute()
        is_new = not bool(_ex.data)
    except Exception:
        is_new = False
    return {"success": True, "message": "ส่ง OTP แล้ว", "is_new": is_new}


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
        # จำกัดจำนวนครั้งที่เดา — กัน brute-force OTP
        stored["attempts"] = int(stored.get("attempts", 0)) + 1
        if stored["attempts"] >= 5:
            del OTP_STORE[phone]
            raise HTTPException(status_code=429, detail="ใส่ OTP ผิดหลายครั้งเกินไป กรุณาขอรหัสใหม่")
        raise HTTPException(status_code=400, detail="OTP ไม่ถูกต้อง")

    # OTP ถูกต้อง — ลบทิ้ง
    del OTP_STORE[phone]

    sb = get_supabase()

    # เช็คว่ามี customer เบอร์นี้ไหม
    res = sb.table("customers").select("*").eq("phone", phone).execute()
    customer = res.data[0] if res.data else None

    if not customer:
        # สมาชิกใหม่ — บังคับกรอกชื่อ + เช็คชื่อซ้ำ
        name = (body.name or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="กรุณากรอกชื่อสำหรับสมัครสมาชิก")
        if len(name) < 2:
            raise HTTPException(status_code=400, detail="ชื่อสั้นเกินไป กรุณากรอกชื่ออย่างน้อย 2 ตัวอักษร")
        # เช็คชื่อซ้ำ — ไม่ให้ตรงกับสมาชิกคนอื่น (ชื่อบน leaderboard ต้องไม่ซ้ำ)
        try:
            dup = sb.table("customers").select("phone").ilike("display_name", name).execute()
            if dup.data:
                raise HTTPException(status_code=409, detail=f'ชื่อ "{name}" มีคนใช้แล้ว กรุณาใช้ชื่ออื่นนะคะ')
        except HTTPException:
            raise
        except Exception as e:
            print(f"[verify-otp] dup-name check error: {e}")
        ins = sb.table("customers").insert({
            "phone":        phone,
            "display_name": name,
            "name":         name,
        }).execute()
        customer = ins.data[0] if ins.data else {"phone": phone, "display_name": name}

        # แจ้ง admin ตอนมีสมาชิกใหม่สมัครด้วยเบอร์โทร
        await send_line_notify(
            ADMIN_LINE_USER_ID,
            f"🆕 สมาชิกใหม่! {name} ({phone}) สมัครผ่านเว็บ velacoldbrew.com"
        )

    token = _make_customer_token(phone=phone, line_user_id=(customer or {}).get("line_user_id") or "")
    return {"success": True, "customer": customer, "token": token}

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

    # ชื่อบนหน้า public — โชว์ชื่อที่ลูกค้าตั้งเอง (name/display_name แก้ได้ในหน้าโปรไฟล์)
    # ไม่โชว์เบอร์โทรเลย (เป็นหน้า public ใครก็เห็น) — ลูกค้าคุมชื่อที่แสดงเองได้
    top_n = [
        {
            "rank":     i + 1,
            "customer": (r["customer"] or "").strip() or "ลูกค้า VeLA",
            "points":   round(r["points"], 1),
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

    # ลูกค้าที่ login LINE → ตั้งรับแจ้งเตือนทาง LINE
    # อัปเกรดค่าเดิม (รวม 'sms' ที่ติดมาจาก default) ให้เป็น 'line' เว้นแต่ตั้งค่าปิดไว้ชัดเจน
    if customer:
        cur_ch = (customer.get("notify_channel") or "").strip().lower()
        if cur_ch not in _NOTIFY_OPT_OUT and cur_ch != "line":
            try:
                sb.table("customers").update({"notify_channel": "line"}).eq("line_user_id", profile["userId"]).execute()
                customer["notify_channel"] = "line"
            except Exception as e:
                print(f"[line-oauth] set notify_channel error: {e}")

    token = _make_customer_token(phone=(customer or {}).get("phone") or "", line_user_id=profile["userId"])
    return {"success": True, "customer": customer, "token": token}
