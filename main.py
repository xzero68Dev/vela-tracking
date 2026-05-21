import os
import time
import httpx
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---- Config ----
THAIPOST_API_KEY = os.getenv("THAIPOST_API_KEY", "")
SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY", "")
TOKEN_URL        = "https://trackapi.thailandpost.co.th/post/api/v1/authenticate/token"
TRACK_URL        = "https://trackapi.thailandpost.co.th/post/api/v1/track"

DONE_STATUSES = {"delivered", "returned"}

# ---- Token cache ----
_token_cache: dict = {"token": None, "expires_at": 0}

# ---- Supabase client ----
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

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
    "401": ("out_for_delivery",  "ออกนำจ่ายแล้ว"),
    "402": ("out_for_delivery",  "ออกนำจ่ายแล้ว"),
    "501": ("delivered",         "จัดส่งสำเร็จ"),
    "502": ("delivered",         "จัดส่งสำเร็จ"),
    "503": ("delivered",         "จัดส่งสำเร็จ"),
    "504": ("delivered",         "จัดส่งสำเร็จ"),
    "600": ("returned",          "ตีกลับ"),
    "601": ("returned",          "ตีกลับ"),
    "602": ("returned",          "ตีกลับ"),
    "603": ("returned",          "ตีกลับ"),
    "700": ("problem",           "มีปัญหา"),
    "701": ("problem",           "มีปัญหา"),
}

def map_status(code: str):
    return STATUS_MAP.get(str(code), ("unknown", f"สถานะ {code}"))


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


async def fetch_tracking(barcode: str) -> dict:
    token = await get_access_token()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            TRACK_URL,
            json={"status": "all", "language": "TH", "barcode": [barcode]},
            headers={"Authorization": f"Token {token}", "Content-Type": "application/json"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"ไปรษณีไทย API: HTTP {resp.status_code} — {resp.text}")
    data = resp.json()
    if not data.get("status"):
        raise HTTPException(status_code=502, detail=data.get("message", "API error"))
    response   = data.get("response", {})
    items      = response.get("items", {})
    track_count = response.get("track_count", {})
    events_raw = items.get(barcode, [])

    events = []
    for e in events_raw:
        code = str(e.get("status", ""))
        status, desc_th = map_status(code)
        events.append({
            "status_code":  code,
            "status":       status,
            "description":  e.get("status_description") or desc_th,
            "datetime":     e.get("status_date"),
            "location":     e.get("location"),
        })

    latest = events[-1] if events else None  # events เรียงเก่า→ใหม่ ดึงอันสุดท้าย
    current_status, current_status_th = map_status(latest["status_code"] if latest else "")
    return {
        "barcode":           barcode,
        "status":            current_status,
        "status_th":         current_status_th,
        "latest_event":      latest,
        "events":            events,
        "track_count_today": track_count.get("count_number"),
        "track_count_limit": track_count.get("track_count_limit"),
    }


# ---- Cron job ----
async def run_cron():
    """เช็คเฉพาะพัสดุที่ is_done = false ทุก 3 ชั่วโมง"""
    print("[cron] เริ่มเช็คสถานะพัสดุที่ยังไม่เสร็จ...")
    sb = get_supabase()
    rows = sb.table("shipments").select("barcode").eq("is_done", False).execute()
    barcodes = [r["barcode"] for r in (rows.data or [])]
    print(f"[cron] พบ {len(barcodes)} รายการที่ต้องเช็ค")

    for barcode in barcodes:
        try:
            result = await fetch_tracking(barcode)
            status    = result["status"]
            is_done   = status in DONE_STATUSES
            latest    = result["latest_event"] or {}
            sb.table("shipments").update({
                "status":          status,
                "status_th":       result["status_th"],
                "latest_location": latest.get("location"),
                "latest_datetime": latest.get("datetime"),
                "is_done":         is_done,
                "last_checked_at": "now()",
            }).eq("barcode", barcode).execute()
            print(f"[cron] {barcode} → {status} {'✓ done' if is_done else ''}")
        except Exception as e:
            print(f"[cron] ERROR {barcode}: {e}")
        await asyncio.sleep(0.5)  # หน่วงนิดนึงไม่ให้ยิง API ถี่เกิน

    print("[cron] เสร็จแล้ว")


# ---- Scheduler ----
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(run_cron, "interval", hours=3, id="tracking_cron")
    scheduler.start()
    print("[scheduler] cron started — ทุก 3 ชั่วโมง")
    yield
    scheduler.shutdown()

app = FastAPI(title="VeLA Tracking API", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---- Models ----
class AddShipmentsRequest(BaseModel):
    barcodes: list[str]

class BulkRequest(BaseModel):
    barcodes: list[str]


# ---- Endpoints ----

@app.get("/health")
async def health():
    return {"status": "ok", "service": "VeLA Tracking API v2"}


@app.get("/track/{barcode}")
async def track_single(barcode: str):
    """เช็คสถานะพัสดุ 1 ชิ้น (real-time)"""
    return await fetch_tracking(barcode.upper().strip())


@app.post("/track/bulk")
async def track_bulk(body: BulkRequest):
    """เช็คสถานะพัสดุหลายชิ้นพร้อมกัน (real-time, สูงสุด 20)"""
    barcodes = [b.upper().strip() for b in body.barcodes if b.strip()]
    if not barcodes:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcodes")
    if len(barcodes) > 20:
        raise HTTPException(status_code=400, detail="ส่งได้สูงสุด 20 เลขต่อครั้ง")
    tasks   = [fetch_tracking(b) for b in barcodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output  = []
    for barcode, result in zip(barcodes, results):
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
    asyncio.create_task(run_cron())
    return {"message": "กำลังเช็คสถานะ... ดูผลได้ที่ /shipments"}
