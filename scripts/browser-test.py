#!/usr/bin/env python3
"""Runs the demo page's self-test in real browsers (Firefox, WebKit = Safari's engine, and an
emulated iPhone) with Playwright, and exits non-zero if any check fails.

  python3 -m venv .venv && .venv/bin/pip install playwright
  .venv/bin/playwright install firefox webkit && sudo .venv/bin/playwright install-deps firefox webkit
  .venv/bin/python scripts/browser-test.py [URL]        # default: https://iroh-bridge.vercel.app/
"""
import asyncio
import json
import sys

from playwright.async_api import async_playwright

URL = sys.argv[1] if len(sys.argv) > 1 else "https://iroh-bridge.vercel.app/"
STATUS = "document.querySelector('[data-testid=status]')?.textContent ?? ''"
SUMMARY = "document.querySelector('[data-testid=selftest-summary]')?.textContent ?? ''"


async def run(browser_type, name, device=None):
    browser = await browser_type.launch()
    context = await browser.new_context(**(device or {}))
    page = await context.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    await page.goto(URL, wait_until="networkidle")
    await page.wait_for_function(f"/^(Connected|Error)/.test({STATUS})", timeout=60_000)
    result = {"browser": name, "version": browser.version, "status": await page.evaluate(STATUS)}
    if result["status"].startswith("Connected"):
        await page.click("[data-testid=run-selftest]")
        await page.wait_for_function(f"/^\\d+\\/\\d+ passed$/.test({SUMMARY})", timeout=120_000)
        result["summary"] = await page.evaluate(SUMMARY)
        items = await page.locator("[data-testid=selftest-results] li").all_text_contents()
        result["failed"] = [i for i in items if i.startswith("❌")]
    result["page_errors"] = errors[:5]
    await browser.close()
    return result


async def main():
    ok = True
    async with async_playwright() as p:
        targets = [
            (p.firefox, "firefox", None),
            (p.webkit, "webkit", None),
            (p.webkit, "iphone (webkit)", p.devices["iPhone 15"]),
        ]
        for browser_type, name, device in targets:
            try:
                r = await run(browser_type, name, device)
            except Exception as e:  # a browser that can't even load the page is a failure too
                r = {"browser": name, "error": str(e)[:300]}
            ok &= bool(r.get("summary")) and not r.get("failed") and not r.get("error")
            print(json.dumps(r, ensure_ascii=False))
    sys.exit(0 if ok else 1)


asyncio.run(main())
