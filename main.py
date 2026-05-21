import os
import time
import httpx
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="VeLA Tracking API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

THAIPOST_API_KEY = os.getenv("THAIPOST_API_KEY", "")
TOKEN_URL = "https://trackapi.thailandpost.co.th/post/api/v1/authenticate/token"
TRACK_URL = "https://trackapi.thailandpost.co.th/post/api/v1/track"

_token_cache: dict = {"token": None, "expires_at": 0}


async def get_access_token() -> str:
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            TOKEN_URL,
            headers={
                "Authorization": f"Token {THAIPOST_API_KEY}",
                "Content-Type": "application/json",
            },
        )
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"ขอ token ไม่สำเร็จ: HTTP {resp.status_code} — {resp.text}",
            )
        data = resp.json()
        token = data.get("token")
        if not token:
            raise HTTPException(status_code=502, detail=f"ไม่ได้รับ token: {data}")

        _token_cache["token"] = token
        _token_cache["expires_at"] = now + 3600
        return token


STATUS_MAP = {
    "101": ("accepted", "รับฝากแล้ว"),
    "102": ("accepted", "รับฝากแล้ว"),
    "103": ("accepted", "รับฝากแล้ว"),
    "201": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "202": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "203": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "204": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "205": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "206": ("in_transit", "ถึงที่ทำการไปรษณีย์"),
    "207": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "208": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "209": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "210": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "211": ("in_transit", "รับเข้าศูนย์คัดแยก"),
    "212": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "301": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "302": ("in_transit", "อยู่ระหว่างขนส่ง"),
    "401": ("out_for_delivery", "ออกนำจ่ายแล้ว"),
    "402": ("out_for_delivery", "ออกนำจ่ายแล้ว"),
    "501": ("delivered", "จัดส่งสำเร็จ"),
    "502": ("delivered", "จัดส่งสำเร็จ"),
    "503": ("delivered", "จัดส่งสำเร็จ"),
    "504": ("delivered", "จัดส่งสำเร็จ"),
    "600": ("returned", "ตีกลับ"),
    "601": ("returned", "ตีกลับ"),
    "602": ("returned", "ตีกลับ"),
    "603": ("returned", "ตีกลับ"),
    "700": ("problem", "มีปัญหา"),
    "701": ("problem", "มีปัญหา"),
}


def map_status(code: str):
    return STATUS_MAP.get(str(code), ("unknown", f"สถานะ {code}"))


class TrackingEvent(BaseModel):
    status_code: str
    status: str
    description: str
    datetime: Optional[str]
    location: Optional[str]


class TrackingResult(BaseModel):
    barcode: str
    status: str
    status_th: str
    latest_event: Optional[TrackingEvent]
    events: list[TrackingEvent]
    track_count_today: Optional[int]
    track_count_limit: Optional[int]


class BulkRequest(BaseModel):
    barcodes: list[str]


@app.get("/health")
async def health():
    return {"status": "ok", "service": "VeLA Tracking API"}


@app.get("/track/{barcode}", response_model=TrackingResult)
async def track_single(barcode: str):
    barcode = barcode.upper().strip()
    if not barcode:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcode")

    token = await get_access_token()

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            TRACK_URL,
            json={"status": "all", "language": "TH", "barcode": [barcode]},
            headers={
                "Authorization": f"Token {token}",
                "Content-Type": "application/json",
            },
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"ไปรษณีไทย API ตอบกลับ HTTP {resp.status_code} — {resp.text}",
        )

    data = resp.json()
    if not data.get("status"):
        raise HTTPException(status_code=502, detail=data.get("message", "API error"))

    response = data.get("response", {})
    items = response.get("items", {})
    track_count = response.get("track_count", {})
    events_raw = items.get(barcode, [])

    events = []
    for e in events_raw:
        code = str(e.get("status", ""))
        status, desc_th = map_status(code)
        events.append(TrackingEvent(
            status_code=code,
            status=status,
            description=e.get("status_description") or desc_th,
            datetime=e.get("status_date"),
            location=e.get("location"),
        ))

    latest = events[0] if events else None
    current_status, current_status_th = map_status(latest.status_code if latest else "")

    return TrackingResult(
        barcode=barcode,
        status=current_status,
        status_th=current_status_th,
        latest_event=latest,
        events=events,
        track_count_today=track_count.get("count_number"),
        track_count_limit=track_count.get("track_count_limit"),
    )


@app.post("/track/bulk")
async def track_bulk(body: BulkRequest):
    barcodes = [b.upper().strip() for b in body.barcodes if b.strip()]
    if not barcodes:
        raise HTTPException(status_code=400, detail="กรุณาระบุ barcodes")
    if len(barcodes) > 20:
        raise HTTPException(status_code=400, detail="ส่งได้สูงสุด 20 เลขต่อครั้ง")

    tasks = [track_single(b) for b in barcodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = []
    for barcode, result in zip(barcodes, results):
        if isinstance(result, Exception):
            output.append({
                "barcode": barcode,
                "status": "error",
                "status_th": "เกิดข้อผิดพลาด",
                "error": str(result),
            })
        else:
            output.append(result.model_dump())

    return {"results": output, "total": len(output)}
