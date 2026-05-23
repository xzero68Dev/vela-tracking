import os
import time
import httpx
import asyncio
from typing import Optional
from datetime import datetime
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
    "303": ("delivered",         "ผู้รับมารับเอง"),
    "304": ("problem",           "ติดต่อผู้รับไม่ได้"),
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
                "last_checked_at": datetime.utcnow().isoformat(),
            }).eq("barcode", barcode).execute()
            print(f"[cron] {barcode} → {status} {'✓ done' if is_done else ''}")
        except Exception as e:
            print(f"[cron] ERROR {barcode}: {e}")
        await asyncio.sleep(0.5)  # หน่วงนิดนึงไม่ให้ยิง API ถี่เกิน

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

@app.post("/admin/import")
async def import_excel(file: UploadFile = File(...)):
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
    stats = {"orders": 0, "shipping": 0, "tracking_added": 0, "accounting": 0, "daily_summary": 0, "tracking_list": []}

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

    for i in range(0, len(order_rows), 50):
        sb.table("orders").upsert(order_rows[i:i+50], on_conflict="order_id").execute()
    stats["orders"] = len(order_rows)

    # ---- Import Shipping ----
    shipping_rows = []
    tracking_to_add = []

    for _, r in df_shipping.iterrows():
        carrier_raw = str(r.get("Carrier") or "").strip()
        if "POST" in carrier_raw.upper() or "SABUY" in carrier_raw.upper():
            carrier = "POST SABUY"
        elif "KEX" in carrier_raw.upper():
            carrier = "KEX"
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

        # เก็บ tracking POST SABUY ไว้เพิ่มใน shipments
        if carrier == "POST SABUY" and tracking and str(tracking).strip():
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
                "margin_pct":    safe_num(get_col(r, "Margin %", "Margin%")),
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
        "message": f"Import สำเร็จ — {stats['orders']} orders, {stats['shipping']} shipping, {stats['tracking_added']} tracking, {stats['accounting']} accounting, {stats['daily_summary']} daily summary"
    }
