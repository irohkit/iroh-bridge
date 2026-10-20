#!/usr/bin/env python3
"""Runs the demo page's self-test in the browsers people actually use, with Playwright, and exits
non-zero if any check fails.

Covers desktop Chrome, Edge, Chromium (Brave/Opera/Vivaldi share it), Firefox and WebKit (Safari's
engine); phones and tablets (iPhone, iPad, Android Chrome, Samsung Internet); and in-app browsers or
private modes where service workers are unavailable (LINE / Facebook on iOS), where the
service-worker checks are expected to be skipped rather than fail.

  python3 -m venv .venv && .venv/bin/pip install playwright
  .venv/bin/playwright install chromium firefox webkit && sudo .venv/bin/playwright install --with-deps chrome msedge
  .venv/bin/python scripts/browser-test.py [URL] [--only NAME]   # default URL: https://iroh-bridge.vercel.app/
"""
import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright

args = [a for a in sys.argv[1:] if not a.startswith("--")]
URL = args[0] if args else "https://iroh-bridge.vercel.app/"
ONLY = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
STATUS = "document.querySelector('[data-testid=status]')?.textContent ?? ''"
SUMMARY = "document.querySelector('[data-testid=selftest-summary]')?.textContent ?? ''"
DONE = re.compile(r"^(\d+)/(\d+) passed( \((\d+) skipped\))?$")

SAMSUNG_UA = (
    "Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) "
    "SamsungBrowser/25.0 Chrome/121.0.0.0 Mobile Safari/537.36"
)
LINE_IOS_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Mobile/15E148 Safari Line/14.10.0"
)
FB_ANDROID_UA = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 "
    "Chrome/124.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/460.0.0.0;]"
)


def targets(p):
    no_sw = {"service_workers": "block"}
    return [
        # name, browser type, launch options, context options, service-worker checks expected to run
        ("chrome (desktop)", p.chromium, {"channel": "chrome"}, {}, True),
        ("edge (desktop)", p.chromium, {"channel": "msedge"}, {}, True),
        ("chromium (brave/opera/vivaldi engine)", p.chromium, {}, {}, True),
        ("firefox (desktop)", p.firefox, {}, {}, True),
        ("safari engine (webkit, desktop)", p.webkit, {}, {}, True),
        ("iphone 15 (safari)", p.webkit, {}, dict(p.devices["iPhone 15"]), True),
        ("ipad (safari)", p.webkit, {}, dict(p.devices["iPad Pro 11"]), True),
        ("android chrome (pixel 7)", p.chromium, {}, dict(p.devices["Pixel 7"]), True),
        ("samsung internet (galaxy s24)", p.chromium, {}, {**p.devices["Galaxy S9+"], "user_agent": SAMSUNG_UA}, True),
        ("firefox private-like (no service worker)", p.firefox, {}, no_sw, False),
        ("line in-app on iphone (no service worker)", p.webkit, {}, {**p.devices["iPhone 15"], "user_agent": LINE_IOS_UA, **no_sw}, False),
        ("facebook in-app on android", p.chromium, {}, {**p.devices["Pixel 7"], "user_agent": FB_ANDROID_UA}, True),
    ]


async def run(name, browser_type, launch, context_opts, expect_sw):
    browser = await browser_type.launch(**launch)
    context = await browser.new_context(**context_opts)
    page = await context.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    await page.goto(URL, wait_until="networkidle")
    await page.wait_for_function(f"/^(Connected|Error)/.test({STATUS})", timeout=60_000)
    result = {"browser": name, "version": browser.version, "status": (await page.evaluate(STATUS))[:60]}
    if result["status"].startswith("Connected"):
        await page.click("[data-testid=run-selftest]")
        await page.wait_for_function(f"/^\\d+\\/\\d+ passed( \\(\\d+ skipped\\))?$/.test({SUMMARY})", timeout=120_000)
        result["summary"] = await page.evaluate(SUMMARY)
        items = await page.locator("[data-testid=selftest-results] li").all_text_contents()
        result["failed"] = [i for i in items if i.startswith("❌")]
        m = DONE.match(result["summary"])
        skipped = int(m.group(4) or 0)
        # Without a service worker exactly its two checks may be skipped; with one, none.
        result["ok"] = not result["failed"] and m.group(1) == m.group(2) and (skipped == 0 if expect_sw else skipped <= 2)
    else:
        result["ok"] = False
    result["page_errors"] = errors[:5]
    await browser.close()
    return result


async def main():
    ok = True
    async with async_playwright() as p:
        for name, browser_type, launch, context_opts, expect_sw in targets(p):
            if ONLY and ONLY not in name:
                continue
            try:
                r = await run(name, browser_type, launch, context_opts, expect_sw)
            except Exception as e:  # a browser that can't even load the page is a failure too
                r = {"browser": name, "ok": False, "error": str(e)[:300]}
            ok &= r["ok"]
            print(json.dumps(r, ensure_ascii=False))
    sys.exit(0 if ok else 1)


asyncio.run(main())
