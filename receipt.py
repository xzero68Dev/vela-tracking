# -*- coding: utf-8 -*-
"""
สร้างใบเสร็จรับเงิน (ไม่มี VAT) เป็น PDF — VeLA Cold Brew
ใช้ reportlab + ฟอนต์ไทย Garuda (bundle ใน assets/fonts/)
รองรับทั้งออเดอร์เว็บ (WEB...) และ Shopee (เลขล้วน)
"""
import io, os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER

# ---- ลงทะเบียนฟอนต์ไทย (ครั้งเดียวตอน import) ----
_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "fonts")
_FONT_OK = False
try:
    pdfmetrics.registerFont(TTFont("Garuda",      os.path.join(_FONT_DIR, "Garuda.ttf")))
    pdfmetrics.registerFont(TTFont("Garuda-Bold", os.path.join(_FONT_DIR, "Garuda-Bold.ttf")))
    from reportlab.pdfbase.pdfmetrics import registerFontFamily
    registerFontFamily("Garuda", normal="Garuda", bold="Garuda-Bold",
                       italic="Garuda", boldItalic="Garuda-Bold")
    _FONT_OK = True
except Exception as e:
    print(f"[receipt] ลงทะเบียนฟอนต์ไม่ได้: {e}")

FONT   = "Garuda"      if _FONT_OK else "Helvetica"
FONT_B = "Garuda-Bold" if _FONT_OK else "Helvetica-Bold"

BRAND = colors.HexColor("#2E75B6")   # โทนเดียวกับ template
ALT   = colors.HexColor("#DBE5F1")
LINE  = colors.HexColor("#BDD7EE")
GREY  = colors.HexColor("#666666")

_TH_NUM = ["ศูนย์","หนึ่ง","สอง","สาม","สี่","ห้า","หก","เจ็ด","แปด","เก้า"]
_TH_POS = ["","สิบ","ร้อย","พัน","หมื่น","แสน","ล้าน"]

def _read_int_thai(n: int) -> str:
    if n == 0:
        return _TH_NUM[0]
    s = ""
    digits = str(n)
    L = len(digits)
    for i, ch in enumerate(digits):
        d = int(ch); pos = L - i - 1
        if d == 0:
            continue
        p = pos % 6
        if p == 0 and pos >= 6:
            s += _TH_POS[6]  # ล้าน
        if p == 1 and d == 1:
            s += _TH_POS[1]                       # สิบ
        elif p == 1 and d == 2:
            s += "ยี่" + _TH_POS[1]                # ยี่สิบ
        elif p == 0 and d == 1 and L > 1 and (L - 1) != i:
            s += "เอ็ด"                            # ...เอ็ด
        else:
            s += _TH_NUM[d] + _TH_POS[p]
    return s

def baht_text(amount: float) -> str:
    """แปลงจำนวนเงินเป็นข้อความภาษาไทย เช่น 258.00 -> 'สองร้อยห้าสิบแปดบาทถ้วน'"""
    try:
        amount = round(float(amount) + 1e-9, 2)
    except Exception:
        return ""
    baht = int(amount)
    satang = int(round((amount - baht) * 100))
    if satang == 0:
        return f"{_read_int_thai(baht)}บาทถ้วน"
    return f"{_read_int_thai(baht)}บาท{_read_int_thai(satang)}สตางค์"

def _money(v) -> str:
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "-"

def build_receipt_pdf(*, shop: dict, receipt_no: str, date_str: str,
                      order_id: str, channel_label: str, status_label: str,
                      customer: dict, items: list,
                      subtotal=None, discount=None, shipping_fee=None,
                      total: float = 0.0, show_prices: bool = True,
                      note: str = "") -> bytes:
    """
    items: [{"name": str, "qty": int, "unit_price": float|None, "amount": float|None}]
    show_prices: True = แสดงคอลัมน์ราคา/หน่วย+จำนวนเงิน, False = แสดงแค่รายการ+จำนวน
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm, topMargin=16*mm, bottomMargin=16*mm,
        title=f"ใบเสร็จรับเงิน {receipt_no}",
    )

    st_shop  = ParagraphStyle("shop",  fontName=FONT_B, fontSize=15, textColor=BRAND, leading=18)
    st_addr  = ParagraphStyle("addr",  fontName=FONT,   fontSize=9.5, textColor=GREY, leading=13)
    st_title = ParagraphStyle("title", fontName=FONT_B, fontSize=16, alignment=TA_RIGHT, leading=19)
    st_meta  = ParagraphStyle("meta",  fontName=FONT,   fontSize=9.5, alignment=TA_RIGHT, leading=14)
    st_h     = ParagraphStyle("h",     fontName=FONT_B, fontSize=10.5, leading=14)
    st_body  = ParagraphStyle("body",  fontName=FONT,   fontSize=10, leading=14)
    st_cell  = ParagraphStyle("cell",  fontName=FONT,   fontSize=9.5, leading=13)
    st_cellR = ParagraphStyle("cellR", fontName=FONT,   fontSize=9.5, leading=13, alignment=TA_RIGHT)
    st_thb   = ParagraphStyle("thb",   fontName=FONT,   fontSize=10, leading=14, textColor=BRAND)
    st_foot  = ParagraphStyle("foot",  fontName=FONT,   fontSize=8.5, leading=12, textColor=GREY)

    elems = []

    # ---- Header: ชื่อร้าน (ซ้าย) + หัวเอกสาร (ขวา) ----
    left = [
        Paragraph(shop.get("name", "VeLA Cold Brew"), st_shop),
        Paragraph(shop.get("address", ""), st_addr),
        Paragraph(f"โทร {shop.get('phone','')}", st_addr),
    ]
    right = [
        Paragraph("ใบเสร็จรับเงิน", st_title),
        Paragraph("RECEIPT", ParagraphStyle("en", fontName=FONT, fontSize=9, alignment=TA_RIGHT, textColor=GREY)),
        Spacer(1, 4),
        Paragraph(f"เลขที่ : <b>{receipt_no}</b>", st_meta),
        Paragraph(f"วันที่ : {date_str}", st_meta),
    ]
    head = Table([[left, right]], colWidths=[100*mm, 74*mm])
    head.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
    elems += [head, Spacer(1, 6),
              HRFlowable(width="100%", thickness=1.2, color=BRAND), Spacer(1, 8)]

    # ---- ข้อมูลลูกค้า + อ้างอิงออเดอร์ ----
    addr_lines = " ".join([x for x in [customer.get("address",""),
                                       customer.get("province",""),
                                       customer.get("zip","")] if x]).strip()
    cust_left = [
        Paragraph("ลูกค้า", ParagraphStyle("lbl", fontName=FONT_B, fontSize=9, textColor=GREY)),
        Paragraph(customer.get("name") or "-", st_h),
    ]
    if addr_lines:
        cust_left.append(Paragraph(addr_lines, st_cell))
    if customer.get("phone"):
        cust_left.append(Paragraph(f"โทร {customer.get('phone')}", st_cell))

    cust_right = [
        Paragraph(f"อ้างอิงออเดอร์ : {order_id}", st_cellR),
        Paragraph(f"ช่องทาง : {channel_label}", st_cellR),
        Paragraph(f"สถานะ : {status_label}", st_cellR),
    ]
    cust = Table([[cust_left, cust_right]], colWidths=[104*mm, 70*mm])
    cust.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
    elems += [cust, Spacer(1, 10)]

    # ---- ตารางรายการสินค้า ----
    if show_prices:
        header = ["#", "รายการ", "จำนวน", "ราคา/หน่วย", "จำนวนเงิน"]
        col_w  = [10*mm, 88*mm, 18*mm, 28*mm, 30*mm]
        rows = [header]
        for i, it in enumerate(items, 1):
            rows.append([
                str(i),
                Paragraph(it.get("name","-"), st_cell),
                str(it.get("qty","")),
                _money(it.get("unit_price")),
                _money(it.get("amount")),
            ])
    else:
        header = ["#", "รายการ", "จำนวน"]
        col_w  = [12*mm, 132*mm, 30*mm]
        rows = [header]
        for i, it in enumerate(items, 1):
            rows.append([str(i), Paragraph(it.get("name","-"), st_cell), str(it.get("qty",""))])

    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    style = [
        ("FONT", (0,0), (-1,0), FONT_B, 10),
        ("FONT", (0,1), (-1,-1), FONT, 9.5),
        ("BACKGROUND", (0,0), (-1,0), BRAND),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (2,0), (-1,-1), "CENTER" if not show_prices else "RIGHT"),
        ("ALIGN", (2,0), (2,-1), "CENTER"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LINEBELOW", (0,0), (-1,0), 0.6, BRAND),
        ("GRID", (0,0), (-1,-1), 0.4, LINE),
    ]
    for r in range(1, len(rows)):
        if r % 2 == 0:
            style.append(("BACKGROUND", (0,r), (-1,r), ALT))
    if show_prices:
        style.append(("ALIGN", (3,0), (4,-1), "RIGHT"))
    tbl.setStyle(TableStyle(style))
    elems += [tbl, Spacer(1, 8)]

    # ---- สรุปยอด ----
    summ = []
    if show_prices and subtotal is not None:
        summ.append(["รวมเป็นเงิน", _money(subtotal)])
    if discount:
        summ.append(["ส่วนลด", "-" + _money(discount)])
    if shipping_fee:
        summ.append(["ค่าจัดส่ง", _money(shipping_fee)])
    summ.append(["ยอดสุทธิ (บาท)", _money(total)])

    summ_rows = [[Paragraph(k, ParagraphStyle("sk", fontName=(FONT_B if k.startswith("ยอดสุทธิ") else FONT),
                                              fontSize=10.5, alignment=TA_RIGHT)),
                  Paragraph(v, ParagraphStyle("sv", fontName=(FONT_B if k.startswith("ยอดสุทธิ") else FONT),
                                              fontSize=10.5, alignment=TA_RIGHT,
                                              textColor=(BRAND if k.startswith("ยอดสุทธิ") else colors.black)))]
                 for k, v in summ]
    summ_tbl = Table(summ_rows, colWidths=[42*mm, 32*mm], hAlign="RIGHT")
    sstyle = [("VALIGN",(0,0),(-1,-1),"MIDDLE"),
              ("TOPPADDING",(0,0),(-1,-1),3), ("BOTTOMPADDING",(0,0),(-1,-1),3),
              ("LINEABOVE",(0,len(summ_rows)-1),(-1,len(summ_rows)-1),0.8,BRAND)]
    summ_tbl.setStyle(TableStyle(sstyle))
    elems += [summ_tbl, Spacer(1, 4)]

    elems += [Paragraph(f"({baht_text(total)})", st_thb), Spacer(1, 14)]

    # ---- ท้ายเอกสาร: สถานะชำระเงิน (กล่องกรอบเขียว) + ลายเซ็น ----
    GREEN = colors.HexColor("#2E9E5B")
    paid_label = Table([[Paragraph("ชำระเงินแล้ว",
                        ParagraphStyle("paid", fontName=FONT_B, fontSize=11,
                                       textColor=GREEN, alignment=TA_CENTER))]],
                       colWidths=[38*mm])
    paid_label.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 1.2, GREEN),
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#EAF7EF")),
        ("TOPPADDING", (0,0), (-1,-1), 6), ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
    ]))
    paid = Table([[
        paid_label,
        Paragraph("ผู้รับเงิน<br/><br/>..................................................<br/>( VeLA Cold Brew )",
                  ParagraphStyle("sign", fontName=FONT, fontSize=9.5, alignment=TA_CENTER, leading=14)),
    ]], colWidths=[100*mm, 74*mm])
    paid.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"BOTTOM")]))
    elems += [paid, Spacer(1, 12)]

    foot = note or "เอกสารนี้เป็นใบเสร็จรับเงิน ออกโดยระบบอัตโนมัติ ไม่ต้องมีลายเซ็นก็ถือเป็นหลักฐานการชำระเงินได้"
    elems += [HRFlowable(width="100%", thickness=0.5, color=LINE), Spacer(1, 4),
              Paragraph(foot, st_foot)]

    doc.build(elems)
    return buf.getvalue()
