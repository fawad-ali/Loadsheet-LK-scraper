"""
PostEx Loadsheet Scraper v5
============================
Back to basics: click the correct span, intercept the real API URL.

ROOT CAUSE of v2/v3 click failures:
  - v2: used `span.orders` — class doesn't exist in the DOM
  - v3: used `td.dt-tracking span` — the class `dt-tracking` doesn't exist
        on the <td> in the REAL rendered HTML. The HTML has inline styles only.

WHAT THE REAL HTML LOOKS LIKE (from Document 3):
  <td class="data-col dt-tracking" style="width: 100px;">
    <span class="smaller-text" style="cursor: pointer; color: blue;"> 28 </span>
  </td>

  BUT — the class "dt-tracking" IS present. The problem was that
  `query_selector` on a Playwright ElementHandle searches WITHIN that element,
  so `row.query_selector("td.dt-tracking span")` should work... UNLESS
  the page hadn't rendered yet (0 tr.data-item rows in v3 meant Angular
  didn't load because api.postex.pk was blocked in the browser).

TRUE ROOT CAUSE:
  The runner blocks api.postex.pk ONLY from Chromium (net::ERR_ABORTED).
  So Angular can't fetch data → table is empty.
  BUT requests.Session can hit api.postex.pk fine.

SOLUTION (v5):
  1. Use browser to log in and capture the token.
  2. Use requests to call the loadsheet LIST page (the HTML page, not API).
     Actually — intercept the network traffic DURING page load at the
     context level using route interception. When Angular makes its
     load-sheet list call on page load, we capture that URL + real IDs.
  3. Alternatively: inject a fetch() call from the page JS context using
     the already-authenticated session — the browser IS authenticated,
     it just can't reach api.postex.pk due to runner network policy.
     We can use page.evaluate() to make the fetch from inside the browser
     using XMLHttpRequest with the token, then return the result to Python.

ACTUALLY THE SIMPLEST FIX:
  The browser has the token. The browser CAN'T reach api.postex.pk.
  Python requests CAN reach api.postex.pk.
  
  We know from the network tab the REAL loadsheet list URL is:
    GET https://api.postex.pk/services/merchant/api/load-sheet-logs/{merchantId}
  or similar. We need to find it.

  The page URL is: https://merchant.postex.pk/main/load-sheet-logs
  The Angular component fetches something on load. We intercept THAT
  at context level (before the browser fails) using page.route() to
  capture the URL pattern, then replay it with requests.

FINAL APPROACH:
  Use page.route() to intercept ALL requests to api.postex.pk,
  log their URLs (we don't need the response, just the URL pattern),
  fulfill them with a fake 200 so Angular doesn't error out,
  then use requests to actually call those URLs with Python.
  
  This gives us the EXACT URL + params the Angular app uses.
"""

import os
import re
import json
import time
import logging
import traceback
from decimal import Decimal

from datetime import datetime, timedelta
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# ────────────────────────────────────────────────────────────────……[...]
# Configuration
# ────────────────────────────────────────────────────────────────……[...]

SAVE_ONLY_LOADSHEET_SUMMARY = True   # Set to True to save only summary, False to save all data
TESTING_ON = False                    # Set to True to scrape a specific date for testing
DEBUG_ON   = False                    # Set to True to enable all logging/screenshots/debug files;
                                     # Set to False for silent production runs


# ────────────────────────────────────────────────────────────────……[...]
# Logging
# ────────────────────────────────────────────────────────────────……[...]

if DEBUG_ON:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)-8s] %(funcName)s:%(lineno)d | %(message)s",
    )
else:
    # Suppress everything — only CRITICAL errors will ever surface
    logging.basicConfig(level=logging.CRITICAL)

log = logging.getLogger("postex-v5")

STEP = 0
def trace(msg, data=None):
    """Log a numbered debug step. No-op when DEBUG_ON is False."""
    if not DEBUG_ON:
        return
    global STEP
    STEP += 1
    prefix = f"[STEP {STEP:06d}]"
    if data is not None:
        try:
            pretty = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        except Exception:
            pretty = str(data)
        log.debug(f"{prefix} {msg}\n{pretty}")
    else:
        log.debug(f"{prefix} {msg}")


# ────────────────────────────────────────────────────────────────…[...]
# Config
# ────────────────────────────────────────────────────────────────…[...]

BASE_URL      = "https://merchant.postex.pk"
LOGIN_URL     = f"{BASE_URL}/login"
LOADSHEET_URL = f"{BASE_URL}/main/load-sheet-logs"
API_HOST      = "api.postex.pk"

USERNAME = os.environ.get("POSTEX_USERNAME", "")
PASSWORD = os.environ.get("POSTEX_PASSWORD", "")

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Debug directory is only created when debugging is active
DEBUG_DIR = OUTPUT_DIR / "debug"
if DEBUG_ON:
    DEBUG_DIR.mkdir(exist_ok=True)


# ────────────────────────────────────────────────────────────────…[...]
# Date
# ────────────────────────────────────────────────────────────────…[...]

from zoneinfo import ZoneInfo

if TESTING_ON:
    TARGET_DATE = datetime(2026, 5, 8)
else:
    DATE_OVERRIDE = os.environ.get("DATE_OVERRIDE")

    if DATE_OVERRIDE:
        TARGET_DATE = datetime.strptime(DATE_OVERRIDE, "%Y-%m-%d")
    else:
        pakistan_now = datetime.now(ZoneInfo("Asia/Karachi"))

        TARGET_DATE = (
            pakistan_now - timedelta(days=1)
        )

print(f"Using target date: {TARGET_DATE.strftime('%Y-%m-%d')}")

DATE_TAG     = TARGET_DATE.strftime("%Y-%m-%d")
TARGET_MONTH = TARGET_DATE.strftime("%b")
TARGET_DAY   = TARGET_DATE.day
TARGET_YEAR  = TARGET_DATE.year
TARGET_LABEL = f"{TARGET_MONTH} {TARGET_DAY}, {TARGET_YEAR}"
OUTPUT_FILE  = OUTPUT_DIR / f"loadsheet_{DATE_TAG}.json"

trace("Config", {"target": TARGET_LABEL, "output": str(OUTPUT_FILE), "save_only_summary": SAVE_ONLY_LOADSHEET_SUMMARY})


# ────────────────────────────────────────────────────────────────…[...]
# Helpers
# ────────────────────────────────────────────────────────────────…[...]

def write_json(path, data):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

def screenshot(page, name):
    """Save a screenshot. No-op when DEBUG_ON is False."""
    if not DEBUG_ON:
        return
    try:
        p = DEBUG_DIR / f"{name}.png"
        page.screenshot(path=str(p), full_page=True)
        trace(f"Screenshot -> {p}")
    except Exception:
        log.exception("screenshot failed")

def dump_html(page, name):
    """Save page HTML to disk. No-op when DEBUG_ON is False."""
    if not DEBUG_ON:
        return
    try:
        html = page.content()
        p = DEBUG_DIR / f"{name}.html"
        p.write_text(html, encoding="utf-8")
        trace(f"HTML -> {p} ({len(html)} chars)")
    except Exception:
        log.exception("html dump failed")

def matches_target_date(text):
    if not text:
        return False
    s = str(text).strip()
    # Epoch ms
    if re.fullmatch(r"\d{13}", s):
        dt = datetime.fromtimestamp(int(s) / 1000)
        return dt.year == TARGET_YEAR and dt.month == TARGET_DATE.month and dt.day == TARGET_DAY
    # ISO
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return int(m.group(1)) == TARGET_YEAR and int(m.group(2)) == TARGET_DATE.month and int(m.group(3)) == TARGET_DAY
    # Human
    m = re.search(r"(\w+)\s+(\d{1,2}),\s+(\d{4})", s)
    if m:
        month, day_s, year_s = m.groups()
        return month == TARGET_MONTH and int(day_s) == TARGET_DAY and int(year_s) == TARGET_YEAR
    return False


def extract_summary_from_orders(orders_data):
    """
    Extract summary from orders API response:
    - total_orders: count of all orders
    - total_invoice_payment: sum of all invoicePayment values
    - order_ref_numbers: array of all orderRefNumber values
    """
    try:
        if not isinstance(orders_data, dict):
            return None
        
        dist = orders_data.get("dist", [])
        if not isinstance(dist, list):
            return None
        
        total_orders = len(dist)
        total_invoice = Decimal("0.00")
        order_refs = []
        
        for order in dist:
            # Sum invoice payments
            invoice_payment = order.get("invoicePayment", "0.00")
            try:
                total_invoice += Decimal(str(invoice_payment))
            except Exception:
                pass
            
            # Collect order ref numbers
            order_ref = order.get("orderRefNumber")
            if order_ref:
                order_refs.append(order_ref)
        
        return {
            "total_orders": total_orders,
            "total_invoice_payment": str(total_invoice),
            "order_ref_numbers": order_refs,
        }
    except Exception as e:
        log.exception("Error extracting summary from orders")
        return None


def prepare_loadsheet_output(row):
    """
    Prepare loadsheet row for output based on SAVE_ONLY_LOADSHEET_SUMMARY setting.
    If True: only include loadsheet_number and summary
    If False: include all data
    """
    if SAVE_ONLY_LOADSHEET_SUMMARY:
        return {
            "loadsheet_number": row.get("loadsheet_number"),
            "summary": row.get("summary"),
        }
    else:
        return row


# ────────────────────────────────────────────────────────────────…[...]
# Main browser session — intercept ALL api.postex.pk requests
# ────────────────────────────────────────────────────────────────…[...]

def run_browser_session():
    """
    Single browser session that:
    1. Logs in
    2. Routes ALL api.postex.pk requests through Python:
       - Captures the URL
       - Fires the real HTTP request using requests (which CAN reach the API)
       - Returns the response body back to the browser so Angular works normally
    3. Navigates to the loadsheet page — Angular loads and populates the table
    4. Waits for and clicks the correct span
    5. Captures the order API URL from the intercepted click request
    6. Returns everything needed
    """

    intercepted_urls   = []   # all api.postex.pk URLs seen
    loadsheet_list_url = None # the URL Angular uses to list loadsheets
    order_api_calls    = []   # URLs from clicking the span
    token              = ""
    merchant_id        = ""
    cookies_list       = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context()
        page    = context.new_page()

        # Browser console/error forwarding only when debugging
        if DEBUG_ON:
            page.on("console",   lambda m: log.debug(f"BROWSER[{m.type}] {m.text}"))
            page.on("pageerror", lambda e: log.debug(f"PAGE ERROR: {e}"))

        # ── Step A: Login ────────────────────────────────────────────────────
        trace("Navigating to login")
        page.goto(LOGIN_URL, wait_until="networkidle")
        screenshot(page, "01_login")
        page.fill('input[type="email"]',    USERNAME)
        page.fill('input[type="password"]', PASSWORD)
        page.click('button[type="submit"]')
        page.wait_for_url(f"{BASE_URL}/main/**", timeout=30_000)
        screenshot(page, "02_post_login")
        trace("Login OK", {"url": page.url})

        # ── Step B: Extract token + merchant_id ──────────────────────────────
        storage = page.evaluate("""() => {
            const ss = {};
            for (let i = 0; i < sessionStorage.length; i++) {
                const k = sessionStorage.key(i);
                ss[k] = sessionStorage.getItem(k);
            }
            return ss;
        }""")
        trace("sessionStorage", storage)

        token       = storage.get("token", "")
        merchant_id = storage.get("merchantId", "")
        cookies_list = context.cookies()

        trace("Auth extracted", {
            "token_len":   len(token),
            "merchant_id": merchant_id,
            "cookies":     len(cookies_list),
        })

        if not token:
            raise RuntimeError("No token found after login")

        # ── Step C: Build a requests.Session for API proxy ───────────────────
        proxy_session = requests.Session()
        proxy_session.headers.update({
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization":   f"Bearer {token}",
            "Origin":          BASE_URL,
            "Referer":         LOADSHEET_URL,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        })
        for c in cookies_list:
            proxy_session.cookies.set(c["name"], c["value"], domain=c.get("domain"))

        # ── Step D: Route ALL api.postex.pk requests through requests ─────────
        def handle_api_route(route, request):
            url     = request.url
            method  = request.method
            headers = dict(request.headers)
            
            trace(f"INTERCEPTED: {method} {url}")
            intercepted_urls.append(url)

            try:
                req_headers = {
                    "Accept":          "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Authorization":   f"Bearer {token}",
                    "Origin":          BASE_URL,
                    "Referer":         LOADSHEET_URL,
                    "User-Agent":      headers.get("user-agent", "Mozilla/5.0"),
                }

                resp = proxy_session.request(
                    method  = method,
                    url     = url,
                    headers = req_headers,
                    data    = request.post_data,
                    timeout = 30,
                    allow_redirects = True,
                )

                body = resp.content

                trace(f"PROXIED RESPONSE: {resp.status_code} for {url}", {
                    "preview": resp.text[:500]
                })

                # Save every API response for debugging (only when DEBUG_ON)
                if DEBUG_ON:
                    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", url)[:100]
                    (DEBUG_DIR / f"proxy_{safe}.json").write_text(
                        resp.text, encoding="utf-8"
                    )

                route.fulfill(
                    status  = resp.status_code,
                    headers = {
                        "Content-Type":                "application/json",
                        "Access-Control-Allow-Origin": "*",
                    },
                    body    = body,
                )

            except Exception as e:
                log.exception(f"Proxy failed for {url}")
                route.fulfill(
                    status  = 200,
                    headers = {"Content-Type": "application/json"},
                    body    = b"{}",
                )

        page.route(f"**/{API_HOST}/**", handle_api_route)

        trace("Route interceptor active — navigating to loadsheet page")

        # ── Step E: Navigate to loadsheet page ───────────────────────────────
        page.goto(LOADSHEET_URL, wait_until="networkidle")
        
        trace(f"Current URL after goto: {page.url}")
        
        if "/load-sheet-logs" not in page.url:
            trace("Not on loadsheet page yet, waiting and retrying")
            time.sleep(3)
            page.goto(LOADSHEET_URL, wait_until="networkidle")
            trace(f"Retried navigation, current URL: {page.url}")

        trace("Waiting for load sheet table to fully appear")

        try:
            page.wait_for_selector("table tbody", timeout=60_000)
            page.wait_for_function("""
                () => {
                    const rows = document.querySelectorAll('table tbody tr.data-item');
                    return rows.length > 0;
                }
            """, timeout=60_000)
            trace("Loadsheet table rows appeared")

        except PWTimeout:
            trace("Timed out waiting for loadsheet table")

        time.sleep(8)
        dump_html(page, "03_loadsheet_page")
        screenshot(page, "03_loadsheet_page")

        # ── Step F: Find and process rows ────────────────────────────────────
        rows = page.query_selector_all("table tbody tr.data-item")
        trace(f"Found {len(rows)} tr.data-item rows")

        if not rows:
            rows = page.query_selector_all("tr.data-item")
            trace(f"Broad selector found {len(rows)} rows")

        matched_rows = []

        for idx, row in enumerate(rows):
            raw_html = row.inner_html()

            # Save per-row HTML only when debugging
            if DEBUG_ON:
                (DEBUG_DIR / f"row_{idx}.html").write_text(raw_html, encoding="utf-8")

            cells = row.query_selector_all("td")
            trace(f"Row {idx}: {len(cells)} cells")

            if len(cells) < 6:
                trace(f"Row {idx}: skipped (only {len(cells)} cells)")
                continue

            def cell_text(n):
                try:
                    return cells[n].inner_text().strip()
                except Exception:
                    return ""

            date_text = cell_text(5)
            status    = cell_text(6).upper()

            trace(f"Row {idx}", {
                "loadsheet": cell_text(0),
                "orders":    cell_text(1),
                "date":      date_text,
                "status":    status,
            })

            if not matches_target_date(date_text):
                trace(f"Row {idx}: date mismatch, skipping")
                continue

            dom_sheet_id = None
            m = re.search(r"more-menu-(\d+)", raw_html)
            if m:
                dom_sheet_id = m.group(1)

            row_data = {
                "row_index":        idx,
                "loadsheet_number": cell_text(0),
                "total_orders":     cell_text(1),
                "date_text":        date_text,
                "status":           status,
                "dom_sheet_id":     dom_sheet_id,
                "real_sheet_id":    None,
                "order_api_url":    None,
            }
            trace(f"Row {idx} matched", row_data)

            # ── Step G: Click the order-count span ──────────────────────────
            click_urls_before = len(intercepted_urls)

            clicked = False
            for sel in [
                "td.dt-tracking span.smaller-text",
                "td.dt-tracking span",
                "span.smaller-text[style*='color: blue']",
                "span[style*='color: blue']",
                "td:nth-child(2) span",
            ]:
                try:
                    el = row.query_selector(sel)
                    if el:
                        txt = el.inner_text().strip()
                        trace(f"Row {idx}: clicking via selector '{sel}', text='{txt}'")
                        el.click()
                        clicked = True
                        break
                except Exception as e:
                    trace(f"Row {idx}: selector '{sel}' failed: {e}")

            if not clicked:
                try:
                    cells[1].click()
                    clicked = True
                    trace(f"Row {idx}: clicked cell[1] directly")
                except Exception as e:
                    trace(f"Row {idx}: cell[1] click failed: {e}")

            if clicked:
                trace(f"Row {idx}: waiting 8s for order API call")
                time.sleep(8)

                new_urls = intercepted_urls[click_urls_before:]
                trace(f"Row {idx}: {len(new_urls)} new API URLs after click", new_urls)

                order_re = re.compile(r"/load-sheet/(\d+)/order")
                for u in new_urls:
                    m2 = order_re.search(u)
                    if m2:
                        row_data["real_sheet_id"] = m2.group(1)
                        row_data["order_api_url"] = u
                        trace(f"Row {idx}: REAL sheet_id = {m2.group(1)}", {"url": u})
                        break

                if not row_data["real_sheet_id"]:
                    trace(f"Row {idx}: real_sheet_id not found in new URLs", new_urls)
                    if DEBUG_ON:
                        write_json(
                            DEBUG_DIR / f"all_intercepted_row{idx}.json",
                            intercepted_urls
                        )
            else:
                trace(f"Row {idx}: could not click any span")

            matched_rows.append(row_data)

        trace("All intercepted API URLs", intercepted_urls)
        if DEBUG_ON:
            write_json(DEBUG_DIR / "all_intercepted_urls.json", intercepted_urls)

        browser.close()

    return matched_rows, proxy_session


# ────────────────────────────────────────────────────────────────…[...]
# Fetch orders using the real URL captured from the interceptor
# ────────────────────────────────────────────────────────────────…[...]

STATUS_OPTIONS = {
    "COMPLETED":  ["delivered", "booked", "return", ""],
    "DISPATCHED": ["booked", "delivered", ""],
    "BOOKED":     ["booked", ""],
    "RETURNED":   ["return", ""],
    "CANCELLED":  ["cancelled", ""],
    "":           ["booked", "delivered", "return", ""],
}


def fetch_orders(session, sheet_id, order_api_url=None, row_status="COMPLETED"):
    """
    If we captured the exact URL from the interceptor, use it directly.
    Otherwise build it from the sheet_id with status option candidates.
    """
    base_url = f"https://{API_HOST}/services/merchant/api/load-sheet/{sheet_id}/order"

    urls_to_try = []
    if order_api_url:
        urls_to_try.append(("(captured)", order_api_url, {}))

    for opt in STATUS_OPTIONS.get(row_status, ["booked", "delivered", "return", ""]):
        params = {"loadSheetId": sheet_id, "direction": "desc"}
        if opt:
            params["orderStatusOption"] = opt
        urls_to_try.append((opt, base_url, params))

    for label, url, params in urls_to_try:
        trace(f"Fetching orders [{label}]", {"url": url, "params": params})
        try:
            r = session.get(url, params=params if params else None, timeout=30)
            raw = r.text

            # Save raw response only when debugging
            if DEBUG_ON:
                (DEBUG_DIR / f"orders_{sheet_id}_{re.sub(r'[^a-z0-9]', '_', label)}.json"
                 ).write_text(raw, encoding="utf-8")

            trace(f"Response {r.status_code}", {"preview": raw[:600]})

            try:
                data = r.json()
            except Exception:
                data = {"raw_text": raw}

            if r.status_code == 200:
                trace(f"SUCCESS with [{label}]")
                return {"status_option": label, "status_code": 200,
                        "url": r.url, "data": data}

        except Exception:
            log.exception(f"Request failed for [{label}]")

    return {"status_option": "all_failed", "status_code": None, "data": {}}


# ────────────────────────────────────────────────────────────────…[...]
# Main
# ────────────────────────────────────────────────────────────────…[...]

def main():
    trace("SCRAPER v5 STARTED", {"target": TARGET_LABEL})

    final = {
        "scrape_date": DATE_TAG,
        "target_date": TARGET_LABEL,
        "loadsheets":  [],
    }

    matched_rows, proxy_session = run_browser_session()

    trace(f"{len(matched_rows)} row(s) matched for {TARGET_LABEL}")

    for row in matched_rows:
        sheet_id      = row.get("real_sheet_id") or row.get("dom_sheet_id")
        order_api_url = row.get("order_api_url")
        row_status    = row.get("status", "COMPLETED")

        trace("Processing row", {
            "sheet_id":      sheet_id,
            "order_api_url": order_api_url,
            "status":        row_status,
        })

        if not sheet_id:
            trace("Skipping — no sheet_id")
            row["api_result"] = {"error": "no sheet_id"}
            row["summary"] = None
            final["loadsheets"].append(prepare_loadsheet_output(row))
            continue

        result = fetch_orders(
            proxy_session,
            sheet_id,
            order_api_url = order_api_url,
            row_status    = row_status,
        )
        row["api_result"] = result
        
        summary = extract_summary_from_orders(result.get("data"))
        row["summary"] = summary
        
        if summary:
            trace(f"Loadsheet {row['loadsheet_number']} Summary", summary)
        
        final["loadsheets"].append(prepare_loadsheet_output(row))

    write_json(OUTPUT_FILE, final)
    trace("DONE", {
        "rows":   len(matched_rows),
        "saved":  len(final["loadsheets"]),
        "output": str(OUTPUT_FILE),
        "save_only_summary": SAVE_ONLY_LOADSHEET_SUMMARY,
    })


if __name__ == "__main__":
    main()
