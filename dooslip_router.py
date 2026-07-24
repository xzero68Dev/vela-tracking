"""
DooSlip — LINE Group Bot สำหรับให้ลูกน้องส่งสลิปในกลุ่ม LINE แล้วบอทอ่าน+บันทึกอัตโนมัติ
รวมเข้ามาใน vela-tracking เป็น router แยก (mount ที่ prefix /dooslip) — ไม่แตะระบบเดิม

ใช้ LINE bot คนละตัวกับ OA ลูกค้าของ vela → env แยก:
  DOOSLIP_LINE_CHANNEL_SECRET / DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN
  ANTHROPIC_API_KEY (Claude Vision อ่านสลิป)
  ใช้ SUPABASE_URL / SUPABASE_KEY เดียวกับ vela (ตาราง slips, shops)

Webhook ของบอท @dooslip ให้ชี้มาที่:  https://vela-tracking.onrender.com/dooslip/webhook
"""
import os
import hashlib
import hmac
import base64
import json
import httpx
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, HTTPException
from fastapi.concurrency import run_in_threadpool

TZ_BANGKOK = timezone(timedelta(hours=7))

DOOSLIP_LINE_CHANNEL_SECRET       = os.getenv("DOOSLIP_LINE_CHANNEL_SECRET", "")
DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN = os.getenv("DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN", "")
ANTHROPIC_API_KEY                 = os.getenv("ANTHROPIC_API_KEY", "")
SUPABASE_URL                      = os.getenv("SUPABASE_URL", "")
# ใช้ service key ฝั่ง server (เลี่ยงปัญหา RLS) ถ้าไม่มีค่อย fallback anon
SUPABASE_KEY                      = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY", "")

# lazy init — ไม่ให้ import แล้ว vela พังถ้า env ยังไม่ตั้ง
_anthropic_client = None
_sb_client = None

def _anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _anthropic_client

def _sb():
    global _sb_client
    if _sb_client is None:
        from supabase import create_client
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


# ── Helpers ─────────────────────────────────────────────
def verify_signature(body: bytes, signature: str) -> bool:
    digest = hmac.new(DOOSLIP_LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def reply_message(reply_token: str, text: str):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN}"}
    httpx.post("https://api.line.me/v2/bot/message/reply", headers=headers, json={
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}],
    })


def push_message(group_id: str, text: str):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN}"}
    httpx.post("https://api.line.me/v2/bot/message/push", headers=headers, json={
        "to": group_id,
        "messages": [{"type": "text", "text": text}],
    })


def get_image_content(message_id: str) -> bytes:
    headers = {"Authorization": f"Bearer {DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN}"}
    res = httpx.get(f"https://api-data.line.me/v2/bot/message/{message_id}/content", headers=headers)
    return res.content


def analyze_slip(image_bytes: bytes) -> dict:
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    message = _anthropic().messages.create(
        model=os.getenv("DOOSLIP_VISION_MODEL", "claude-sonnet-4-6"),
        max_tokens=1000,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_b64}},
                {"type": "text", "text": """วิเคราะห์สลิปโอนเงินนี้และตอบเป็น JSON เท่านั้น ห้ามมีข้อความอื่น:
{
  "is_slip": true/false,
  "amount": ยอดเงิน (number หรือ null),
  "transfer_time": "HH:MM" หรือ null,
  "transfer_date": "YYYY-MM-DD" หรือ null,
  "ref_number": "เลขอ้างอิง" หรือ null,
  "sender_name": "ชื่อผู้โอน" หรือ null,
  "receiver_name": "ชื่อบัญชีปลายทาง" หรือ null,
  "bank": "ชื่อธนาคาร" หรือ null
}"""},
            ],
        }],
    )
    raw = message.content[0].text.strip().replace("```json", "").replace("```", "").strip()
    return json.loads(raw)


def check_duplicate(ref_number: str, group_id: str) -> bool:
    if not ref_number:
        return False
    result = _sb().table("slips").select("id").eq("ref_number", ref_number).eq("group_id", group_id).execute()
    return len(result.data) > 0


def check_time_validity(transfer_date: str, transfer_time: str):
    if not transfer_date or not transfer_time:
        return True, ""
    try:
        transfer_dt = datetime.strptime(f"{transfer_date} {transfer_time}", "%Y-%m-%d %H:%M")
        now = datetime.now(TZ_BANGKOK).replace(tzinfo=None)
        diff = now - transfer_dt
        if diff > timedelta(hours=2):
            return False, f"โอนเมื่อ {int(diff.total_seconds() / 3600)} ชั่วโมงที่แล้ว"
        if diff < timedelta(minutes=-5):
            return False, "เวลาในสลิปยังมาไม่ถึง"
        return True, ""
    except Exception:
        return True, ""


def check_receiver(receiver_name: str, account_name: str) -> bool:
    if not receiver_name or not account_name:
        return True
    prefixes = ["นาย", "นาง", "นางสาว", "Mr.", "Mrs.", "Ms.", "น.ส.", "บจก.", "หจก."]
    def normalize(name: str) -> str:
        name = name.strip().lower()
        for p in prefixes:
            name = name.replace(p.lower(), "")
        return name.replace(" ", "")
    return normalize(receiver_name) == normalize(account_name)


def save_slip(data: dict, group_id: str, user_id: str, display_name: str):
    _sb().table("slips").insert({
        "group_id": group_id, "user_id": user_id, "display_name": display_name,
        "amount": data.get("amount"), "transfer_date": data.get("transfer_date"),
        "transfer_time": data.get("transfer_time"), "ref_number": data.get("ref_number"),
        "sender_name": data.get("sender_name"), "receiver_name": data.get("receiver_name"),
        "bank": data.get("bank"), "created_at": datetime.now(TZ_BANGKOK).isoformat(),
    }).execute()


def get_shop(group_id: str):
    result = _sb().table("shops").select("shop_name, account_name, approved").eq("group_id", group_id).execute()
    return result.data[0] if result.data else None


def is_approved(group_id: str) -> bool:
    result = _sb().table("shops").select("approved").eq("group_id", group_id).execute()
    return result.data[0].get("approved", False) if result.data else False


def register_group(group_id: str):
    existing = _sb().table("shops").select("id").eq("group_id", group_id).execute()
    if not existing.data:
        _sb().table("shops").insert({"group_id": group_id, "shop_name": "", "approved": False}).execute()


def set_shop_field(group_id: str, field: str, value: str):
    existing = _sb().table("shops").select("id").eq("group_id", group_id).execute()
    if existing.data:
        _sb().table("shops").update({field: value}).eq("group_id", group_id).execute()
    else:
        _sb().table("shops").insert({"group_id": group_id, field: value, "shop_name": "", "approved": False}).execute()


# ── ประมวลผล 1 event (sync — รันใน threadpool ไม่ให้บล็อก event loop ของ vela) ──
def handle_event(event: dict):
    source_type = event.get("source", {}).get("type")
    if source_type != "group":
        return

    group_id = event["source"]["groupId"]
    event_type = event.get("type")

    if event_type == "join":
        register_group(group_id)
        push_message(group_id,
            f"👋 สวัสดีครับ! นี่คือ DooSlip\n"
            f"──────────────\n"
            f"📋 Group ID ของกลุ่มนี้:\n{group_id}\n"
            f"──────────────\n"
            f"กรุณาแจ้ง ID นี้กับ DooSlip เพื่อเปิดใช้งาน\n"
            f"📱 LINE: @dooslip"
        )
        return

    if event_type != "message":
        return

    user_id = event["source"].get("userId", "unknown")
    msg_type = event["message"].get("type")
    reply_token = event["replyToken"]

    # whitelist check
    if not is_approved(group_id):
        reply_message(reply_token,
            f"⛔ กลุ่มนี้ยังไม่ได้เปิดใช้งาน\n"
            f"กรุณาแจ้ง Group ID กับ DooSlip\n"
            f"──────────────\n"
            f"Group ID: {group_id}\n"
            f"📱 LINE: @dooslip"
        )
        return

    # text commands
    if msg_type == "text":
        text = event["message"].get("text", "").strip()
        if text.startswith("/setshop "):
            name = text.replace("/setshop ", "").strip()
            if name:
                set_shop_field(group_id, "shop_name", name)
                reply_message(reply_token, f"✅ ตั้งชื่อร้านเป็น \"{name}\" เรียบร้อยแล้ว")
            else:
                reply_message(reply_token, "❌ เช่น /setshop ข้าวมันไก่อารีย์")
        elif text.startswith("/setaccount "):
            name = text.replace("/setaccount ", "").strip()
            if name:
                set_shop_field(group_id, "account_name", name)
                reply_message(reply_token, f"✅ ตั้งชื่อบัญชีเป็น \"{name}\" เรียบร้อยแล้ว")
            else:
                reply_message(reply_token, "❌ เช่น /setaccount ศรัณย์พงศ์ ลิ้วล่อง")
        elif text == "/info":
            shop = get_shop(group_id)
            if shop:
                reply_message(reply_token,
                    f"🏪 ชื่อร้าน: {shop.get('shop_name') or 'ยังไม่ได้ตั้ง'}\n"
                    f"👤 ชื่อบัญชี: {shop.get('account_name') or 'ยังไม่ได้ตั้ง'}"
                )
            else:
                reply_message(reply_token, "ยังไม่มีข้อมูลร้าน")
        return

    # image slip
    if msg_type != "image":
        return

    message_id = event["message"]["id"]

    try:
        headers = {"Authorization": f"Bearer {DOOSLIP_LINE_CHANNEL_ACCESS_TOKEN}"}
        profile_res = httpx.get(f"https://api.line.me/v2/bot/group/{group_id}/member/{user_id}", headers=headers)
        display_name = profile_res.json().get("displayName", "ไม่ทราบชื่อ")
    except Exception:
        display_name = "ไม่ทราบชื่อ"

    try:
        image_bytes = get_image_content(message_id)
    except Exception:
        reply_message(reply_token, "❌ ดึงรูปไม่ได้ กรุณาลองใหม่")
        return

    try:
        slip_data = analyze_slip(image_bytes)
    except Exception:
        reply_message(reply_token, "❌ อ่านสลิปไม่ได้ กรุณาลองใหม่")
        return

    if not slip_data.get("is_slip"):
        reply_message(reply_token, "❌ ไม่พบสลิปในรูปนี้\nกรุณาส่งรูปสลิปการโอนเงิน")
        return

    if check_duplicate(slip_data.get("ref_number"), group_id):
        reply_message(reply_token, "⚠️ สลิปนี้ถูกบันทึกไปแล้ว\nกรุณาตรวจสอบอีกครั้ง")
        return

    # เช็คเวลา (ปิดไว้ตามของเดิม — เปิดได้โดยเอา comment ออก)
    # time_ok, time_msg = check_time_validity(slip_data.get("transfer_date"), slip_data.get("transfer_time"))
    # if not time_ok:
    #     reply_message(reply_token, f"⚠️ สลิปเก่าเกินไป ({time_msg})\nกรุณาให้ลูกค้าส่งสลิปใหม่")
    #     return

    shop = get_shop(group_id)
    shop_name = shop.get("shop_name", "") if shop else ""
    account_name = shop.get("account_name", "") if shop else ""
    receiver_name = slip_data.get("receiver_name", "")

    if not check_receiver(receiver_name, account_name):
        reply_message(reply_token,
            f"🚨 สลิปอาจไม่ถูกต้อง!\n"
            f"──────────────\n"
            f"บัญชีปลายทางในสลิป: {receiver_name}\n"
            f"บัญชีร้าน: {account_name}\n"
            f"──────────────\n"
            f"กรุณาตรวจสอบก่อนส่งสินค้า"
        )
        return

    try:
        save_slip(slip_data, group_id, user_id, display_name)
    except Exception:
        reply_message(reply_token, "❌ บันทึกไม่สำเร็จ กรุณาลองใหม่")
        return

    amount = slip_data.get("amount")
    shop_line = f"🏪 {shop_name}\n" if shop_name else ""
    reply_text = (
        f"✅ บันทึกแล้ว\n"
        f"──────────────\n"
        f"{shop_line}"
        f"💰 ยอด: ฿{amount:,.2f}\n"
        f"🕐 เวลา: {slip_data.get('transfer_time', 'ไม่ทราบ')} น.\n"
        f"👤 ผู้โอน: {slip_data.get('sender_name', 'ไม่ทราบ')}\n"
        f"🏦 {slip_data.get('bank', '')}\n"
        f"📋 อ้างอิง: {slip_data.get('ref_number', '')}\n"
        f"──────────────\n"
        f"บันทึกโดย: {display_name}"
    )
    reply_message(reply_token, reply_text)


# ── Router (mount ที่ /dooslip) ──────────────────────────
router = APIRouter()

@router.get("/")
def dooslip_root():
    return {"status": "DooSlip is running 🟢 (on vela-tracking)"}

@router.get("/health")
def dooslip_health():
    return {"status": "ok"}

@router.post("/webhook")
async def dooslip_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body)
    for event in payload.get("events", []):
        # รันงานหนัก (ดึงรูป + Claude Vision + DB) ใน threadpool ไม่ให้บล็อก event loop
        try:
            await run_in_threadpool(handle_event, event)
        except Exception as e:
            print(f"[dooslip] handle_event error: {e}")
    return {"status": "ok"}
