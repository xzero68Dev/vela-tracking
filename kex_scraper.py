"""
KEX Express scraper ใช้ Playwright
ดึงสถานะพัสดุจาก th.kex-express.com โดยตรง ฟรี ไม่มีค่า API
"""
import asyncio
from playwright.async_api import async_playwright

KEX_STATUS_MAP = {
    "จัดส่งพัสดุสำเร็จ":                            ("delivered",        "จัดส่งสำเร็จ"),
    "จัดส่งสำเร็จ":                                  ("delivered",        "จัดส่งสำเร็จ"),
    "กำลังจัดส่งพัสดุ":                             ("out_for_delivery", "กำลังจัดส่ง"),
    "พนักงานติดต่อผู้รับ":                          ("out_for_delivery", "กำลังจัดส่ง"),
    "พัสดุถึงคลังสินค้าปลายทาง":                   ("in_transit",       "ถึงคลังปลายทาง"),
    "พัสดุออกจากศูนย์กระจายสินค้า":               ("in_transit",       "อยู่ระหว่างขนส่ง"),
    "พัสดุถึงศูนย์กระจายสินค้า":                   ("in_transit",       "อยู่ระหว่างขนส่ง"),
    "พนักงานเข้ารับพัสดุแล้ว":                     ("accepted",         "รับพัสดุแล้ว"),
    "ผู้ส่งมาส่งพัสดุที่สาขา":                      ("accepted",         "รับฝากแล้ว"),
    "ผู้ส่งมาส่งพัสดุที่สาขาเคอีเอ็กซ์ พาร์ทเนอร์": ("accepted",      "รับฝากแล้ว"),
    "จัดส่งไม่สำเร็จ":                             ("problem",          "จัดส่งไม่สำเร็จ"),
    "ส่งคืนต้นทาง":                                 ("returned",         "ส่งคืนต้นทาง"),
}


async def fetch_kex_tracking(barcode: str) -> dict:
    """Scrape KEX tracking page และดึงสถานะพัสดุ"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
            ]
        )
        try:
            context = await browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                )
            )
            page = await context.new_page()

            url = f"https://th.kex-express.com/th/track/?track={barcode}"
            await page.goto(url, wait_until='networkidle', timeout=30000)

            # รอให้ tracking results โหลด
            try:
                await page.wait_for_selector('li.status-line', timeout=20000)
            except Exception:
                return {
                    "barcode":   barcode,
                    "status":    "unknown",
                    "status_th": "ไม่พบข้อมูล",
                    "events":    [],
                }

            # ดึง events ทั้งหมด
            events = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.status-line');
                return [...items].map(item => {
                    const statusEl = item.querySelector('.header.bold span');
                    const status   = statusEl ? statusEl.innerText.trim() : '';

                    const lights   = [...item.querySelectorAll('.text-1418.light')];
                    const location = lights[0] ? lights[0].innerText.trim() : '';
                    const dateTxt  = lights.find(el => el.innerText.includes('วันที่'));
                    const timeTxt  = lights.find(el => el.innerText.includes('เวลา'));
                    const date     = dateTxt ? dateTxt.innerText.replace('วันที่', '').trim() : '';
                    const time     = timeTxt ? timeTxt.innerText.replace('เวลา', '').trim() : '';

                    return {
                        description: status,
                        location:    location,
                        date:        date,
                        time:        time,
                        datetime:    (date + ' ' + time).trim()
                    };
                }).filter(e => e.description);
            }
            """)

            if not events:
                return {
                    "barcode":   barcode,
                    "status":    "unknown",
                    "status_th": "ไม่พบข้อมูล",
                    "events":    [],
                }

            # สถานะล่าสุดจาก event แรก (KEX เรียงจากใหม่ไปเก่า)
            latest_desc = events[0]["description"]
            status, status_th = KEX_STATUS_MAP.get(
                latest_desc, ("in_transit", latest_desc)
            )

            # เรียงกลับให้เก่าสุดอยู่ก่อน (เหมือน Thailand Post)
            events_sorted = list(reversed(events))

            return {
                "barcode":         barcode,
                "status":          status,
                "status_th":       status_th,
                "latest_location": events[0].get("location", ""),
                "events":          events_sorted,
            }

        except Exception as e:
            print(f"[KEX scraper] error {barcode}: {e}")
            return {
                "barcode":   barcode,
                "status":    "error",
                "status_th": "เชื่อมต่อไม่ได้",
                "events":    [],
            }
        finally:
            await browser.close()


# ทดสอบ manual
if __name__ == "__main__":
    import sys, json
    code = sys.argv[1] if len(sys.argv) > 1 else "SXF112970001471"
    result = asyncio.run(fetch_kex_tracking(code))
    print(json.dumps(result, ensure_ascii=False, indent=2))
