"""
Playwright base class — user agent rotation, random delays, raw HTML archiving,
and a shared browser context used across all scraping sessions.
"""
from __future__ import annotations
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from playwright.sync_api import (
    sync_playwright, Browser, BrowserContext, Page, Playwright, TimeoutError as PWTimeout
)
from loguru import logger

USER_AGENTS = [
    # Chrome 124 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome 123 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome 122 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Firefox 125 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    # Firefox 124 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0",
    # Safari 17 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # Edge 124 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Chrome 124 Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

VIEWPORT_PROFILES = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1280, "height": 800},
    {"width": 2560, "height": 1440},
]

RAW_HTML_DIR = Path("./data/raw_html")


def _extract_next_data(html: str) -> Optional[Dict]:
    """Extract __NEXT_DATA__ JSON embedded by Next.js (domain.com.au, REA)."""
    import json as _json
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            return _json.loads(m.group(1))
        except _json.JSONDecodeError:
            pass
    return None


def random_delay(min_s: float = 3.0, max_s: float = 8.0) -> None:
    delay = random.uniform(min_s, max_s)
    logger.debug(f"Delay {delay:.1f}s")
    time.sleep(delay)


def save_raw_html(html: str, site: str, label: str) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = RAW_HTML_DIR / site
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_label = re.sub(r"[^\w\-]", "_", label)
    path = out_dir / f"{safe_label}_{ts}.html"
    path.write_text(html, encoding="utf-8", errors="replace")
    return path


def pick_user_agent() -> str:
    return random.choice(USER_AGENTS)


def pick_viewport() -> dict:
    return random.choice(VIEWPORT_PROFILES)


class PlaywrightSession:
    """
    Context-manager that owns a Playwright browser + one BrowserContext per session.
    Each call to new_page() returns a fresh Page with a rotated UA and viewport.
    Keeps a pool of contexts so pages across the same session share cookies/state
    only when explicitly desired; otherwise open a fresh context.
    """

    def __init__(self, headless: bool = True, slow_mo: int = 0):
        self.headless = headless
        self.slow_mo = slow_mo
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None

    def __enter__(self) -> "PlaywrightSession":
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        logger.debug("Playwright browser launched")
        return self

    def __exit__(self, *_) -> None:
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        logger.debug("Playwright browser closed")

    def new_context(self) -> BrowserContext:
        ua = pick_user_agent()
        vp = pick_viewport()
        ctx = self._browser.new_context(
            user_agent=ua,
            viewport=vp,
            locale="en-AU",
            timezone_id="Australia/Sydney",
            geolocation={"latitude": -33.8688, "longitude": 151.2093},
            permissions=["geolocation"],
            extra_http_headers={
                "Accept-Language": "en-AU,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "DNT": "1",
            },
        )
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)
        return ctx

    def fetch_page(
        self,
        url: str,
        site: str,
        html_label: str,
        wait_selector: Optional[str] = None,
        wait_ms: int = 8000,
        retries: int = 3,
    ) -> Optional[str]:
        """
        Opens url in a fresh context, waits for content, saves raw HTML.
        Returns the page HTML or None on permanent failure.
        """
        for attempt in range(1, retries + 1):
            ctx = self.new_context()
            page: Page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)

                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=wait_ms)
                    except PWTimeout:
                        logger.debug(f"Selector '{wait_selector}' not found on {url}")

                # Let lazy-loaded content settle
                page.wait_for_load_state("networkidle", timeout=10000)

                html = page.content()
                save_raw_html(html, site, html_label)
                logger.debug(f"Fetched {url} ({len(html):,} bytes)")
                return html

            except Exception as e:
                logger.warning(f"Attempt {attempt}/{retries} failed for {url}: {e}")
                if attempt < retries:
                    random_delay(5, 12)
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass

        logger.error(f"All {retries} attempts failed for {url}")
        return None
