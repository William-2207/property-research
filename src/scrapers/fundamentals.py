"""
Monthly suburb fundamentals scraper.
Sources:
  - domain.com.au/research/suburb-profile/ (Playwright) — median prices,
    clearance rates, days-on-market, stock levels
  - realestate.com.au/neighbourhoods/ (Playwright) — vacancy, rent, yield
  - SQM Research (requests) — vacancy rate, stock on market
  - ABS (requests) — population growth, income
Raw HTML archived to ./data/raw_html/suburb_profiles/ for debugging.
"""
from __future__ import annotations
import json
import re
import time
from datetime import datetime
from typing import Dict, Optional
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session, upsert_suburb
from ..database.models import Suburb, SuburbFundamentals
from .playwright_base import PlaywrightSession, random_delay
from .suburb_list import SYDNEY_SUBURBS, MELBOURNE_SUBURBS

CITY_STATES = {"sydney": "NSW", "melbourne": "VIC"}

DOMAIN_PROFILE_BASE = "https://www.domain.com.au/research/suburb-profile"
REA_SUBURB_BASE = "https://www.realestate.com.au/neighbourhoods"
SQM_BASE = "https://sqmresearch.com.au/vacancy-rates.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-AU,en;q=0.9",
}


# ─── Domain suburb profile ────────────────────────────────────────────────────

def _build_domain_profile_url(suburb: str, state: str) -> str:
    slug = suburb.lower().replace(" ", "-")
    return f"{DOMAIN_PROFILE_BASE}/{slug}-{state.lower()}"


def _parse_domain_profile_json(data: Dict, asset_type: str) -> Dict:
    """
    Extract fundamentals from __NEXT_DATA__ on a domain suburb profile page.
    domain.com.au embeds all chart data in __NEXT_DATA__ under
    props.pageProps.suburb or similar paths.
    """
    result: Dict = {}
    try:
        props = data.get("props", {}).get("pageProps", {})
        suburb_data = (props.get("suburb") or
                       props.get("suburbData") or
                       props.get("componentProps", {}).get("suburb") or {})

        # Try both house and unit keys
        asset_key = "house" if asset_type == "house" else "unit"
        stats = (suburb_data.get(f"{asset_key}Stats") or
                 suburb_data.get("stats", {}).get(asset_key) or
                 suburb_data.get(asset_key) or {})

        # Median price
        result["median_price"] = (
            stats.get("medianSalePrice") or
            stats.get("median") or
            suburb_data.get(f"median{asset_key.title()}Price")
        )

        # 12-month change for price momentum
        price_change = (stats.get("priceChange12Month") or
                        stats.get("medianSalePriceGrowth"))
        if price_change and result.get("median_price"):
            mp = result["median_price"]
            result["median_price_12m_ago"] = mp / (1 + price_change / 100)
            result["price_momentum_pct"] = price_change

        # Clearance rate
        result["auction_clearance_rate"] = (
            stats.get("clearanceRate") or
            stats.get("auctionClearanceRate") or
            suburb_data.get("clearanceRate")
        )

        # Days on market
        result["days_on_market"] = (
            stats.get("daysOnMarket") or
            stats.get("medianDaysOnMarket")
        )

        # Supply/stock
        result["stock_on_market"] = (
            stats.get("totalListings") or
            stats.get("supplyCount") or
            suburb_data.get("totalListings")
        )

        # Yield
        result["gross_yield_pct"] = (
            stats.get("rentalYield") or
            stats.get("grossYield") or
            suburb_data.get("rentalYield")
        )

    except Exception as e:
        logger.debug(f"Domain profile JSON parse error: {e}")
    return result


def _parse_domain_profile_dom(html: str, asset_type: str) -> Dict:
    """Fallback DOM parse for domain suburb profile pages."""
    result: Dict = {}
    soup = BeautifulSoup(html, "lxml")

    # Look for stat cards like "$850,000 Median price"
    for el in soup.select("[class*='stat'], [class*='Stat'], [class*='metric'], [class*='Metric']"):
        text = el.get_text(separator=" ", strip=True)
        price_m = re.search(r"\$([\d,]+)", text)
        if price_m and ("median" in text.lower() or "price" in text.lower()):
            val = float(price_m.group(1).replace(",", ""))
            if result.get("median_price") is None:
                result["median_price"] = val

        pct_m = re.search(r"([\d.]+)%", text)
        if pct_m:
            pct = float(pct_m.group(1))
            if "clearance" in text.lower() and result.get("auction_clearance_rate") is None:
                result["auction_clearance_rate"] = pct
            elif "yield" in text.lower() and result.get("gross_yield_pct") is None:
                result["gross_yield_pct"] = pct

        days_m = re.search(r"(\d+)\s*day", text, re.I)
        if days_m and "market" in text.lower():
            result["days_on_market"] = int(days_m.group(1))

    return result


def scrape_domain_suburb_profile(session_pw: PlaywrightSession,
                                  suburb: str, state: str,
                                  asset_type: str) -> Dict:
    url = _build_domain_profile_url(suburb, state)
    label = f"suburb_domain_{suburb}_{state}_{asset_type}"
    html = session_pw.fetch_page(
        url, "suburb_profiles", label,
        wait_selector="[class*='suburb-profile'], [class*='SuburbProfile'], main",
        wait_ms=10000,
    )
    if not html:
        return {}

    from .playwright_base import _extract_next_data
    next_data = _extract_next_data(html)
    if next_data:
        parsed = _parse_domain_profile_json(next_data, asset_type)
        if parsed.get("median_price"):
            return parsed

    return _parse_domain_profile_dom(html, asset_type)


# ─── REA suburb profile ───────────────────────────────────────────────────────

def _build_rea_suburb_url(suburb: str, state: str) -> str:
    slug = suburb.lower().replace(" ", "-")
    return f"{REA_SUBURB_BASE}/{slug}-{state.lower()}"


def scrape_rea_suburb_profile(session_pw: PlaywrightSession,
                               suburb: str, state: str) -> Dict:
    url = _build_rea_suburb_url(suburb, state)
    label = f"suburb_rea_{suburb}_{state}"
    html = session_pw.fetch_page(
        url, "suburb_profiles_rea", label,
        wait_selector="[class*='suburb'], main",
        wait_ms=8000,
    )
    if not html:
        return {}

    result: Dict = {}
    soup = BeautifulSoup(html, "lxml")

    # REA embeds suburb stats in __NEXT_DATA__ too
    script = soup.find("script", id="__NEXT_DATA__")
    if script:
        try:
            data = json.loads(script.string or "")
            props = data.get("props", {}).get("pageProps", {})
            nb = (props.get("neighbourhood") or
                  props.get("suburbProfile") or {})
            result["vacancy_rate"] = nb.get("vacancyRate")
            result["median_weekly_rent"] = (nb.get("medianRentalPrice") or
                                            nb.get("medianWeeklyRent"))
            result["rent_growth_12m_pct"] = nb.get("rentalPriceGrowth12Month")
            result["gross_yield_pct"] = nb.get("grossRentalYield")
        except (json.JSONDecodeError, AttributeError):
            pass

    # DOM fallback
    if not result.get("vacancy_rate"):
        for el in soup.select("[class*='stat'], [class*='Stat']"):
            t = el.get_text(separator=" ", strip=True)
            pct_m = re.search(r"([\d.]+)%", t)
            if pct_m:
                pct = float(pct_m.group(1))
                if "vacanc" in t.lower():
                    result["vacancy_rate"] = pct
                elif "yield" in t.lower():
                    result.setdefault("gross_yield_pct", pct)
        rent_els = soup.select("[class*='rent'], [class*='Rent']")
        for el in rent_els:
            m = re.search(r"\$\s*([\d,]+)", el.get_text())
            if m:
                result["median_weekly_rent"] = float(m.group(1).replace(",", ""))
                break

    return result


# ─── SQM Research (requests — lightweight) ───────────────────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=3, max=10))
def scrape_sqm_vacancy(suburb: str, state: str) -> Dict:
    try:
        url = f"{SQM_BASE}?region={state}&window=12&t=1"
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return {}
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.select("table.sqmTable tr"):
            cells = row.find_all("td")
            if len(cells) >= 3 and suburb.lower() in cells[0].text.lower():
                try:
                    return {
                        "vacancy_rate": float(cells[1].text.strip().replace("%", "")),
                        "stock_on_market": int(cells[2].text.strip().replace(",", "")),
                    }
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        logger.debug(f"SQM scrape failed {suburb}: {e}")
    return {}


# ─── LGA helpers ─────────────────────────────────────────────────────────────

def _resolve_lga(suburb_name: str, city: str) -> str:
    NSW = {
        "Parramatta": "City of Parramatta", "Strathfield": "Strathfield Council",
        "Burwood": "Burwood Council", "Auburn": "Cumberland Council",
        "Bankstown": "Canterbury-Bankstown Council", "Hurstville": "Georges River Council",
        "Penrith": "Penrith City Council", "Blacktown": "Blacktown City Council",
        "Liverpool": "Liverpool City Council", "Hornsby": "Hornsby Shire Council",
        "Ryde": "City of Ryde", "Chatswood": "Willoughby City Council",
        "Manly": "Northern Beaches Council", "Fairfield": "Fairfield City Council",
        "Sutherland": "Sutherland Shire", "Campbelltown": "Campbelltown City Council",
        "Burwood": "Burwood Council", "Merrylands": "Cumberland Council",
        "Granville": "Cumberland Council", "Homebush": "Strathfield Council",
        "Greenacre": "Canterbury-Bankstown Council", "Lidcombe": "Cumberland Council",
        "Newtown": "Inner West Council", "Marrickville": "Inner West Council",
        "Ashfield": "Inner West Council", "Randwick": "Randwick City Council",
        "Kingsford": "Randwick City Council", "Maroubra": "Randwick City Council",
        "Mascot": "Sydney City Council", "Waterloo": "Sydney City Council",
        "Redfern": "Sydney City Council", "Alexandria": "Sydney City Council",
    }
    VIC = {
        "Footscray": "City of Maribyrnong", "Sunshine": "City of Brimbank",
        "Werribee": "City of Wyndham", "Dandenong": "City of Greater Dandenong",
        "Box Hill": "City of Whitehorse", "Ringwood": "City of Maroondah",
        "Preston": "City of Darebin", "Coburg": "City of Moreland",
        "Brunswick": "City of Moreland", "Northcote": "City of Darebin",
        "Frankston": "City of Frankston", "Cranbourne": "City of Casey",
        "Epping": "City of Whittlesea", "Thomastown": "City of Whittlesea",
        "Clayton": "City of Monash", "Glen Waverley": "City of Monash",
        "Oakleigh": "City of Monash", "Cheltenham": "City of Kingston",
        "Springvale": "City of Greater Dandenong",
        "Noble Park": "City of Greater Dandenong",
        "Moorabbin": "City of Kingston", "Carnegie": "City of Glen Eira",
        "Bentleigh": "City of Glen Eira", "Yarraville": "City of Maribyrnong",
    }
    return (NSW if city == "sydney" else VIC).get(suburb_name, "Unknown LGA")


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def _process_suburb(cfg: Config, suburb_name: str, state: str, city: str,
                    snapshot_month: str, session_pw: PlaywrightSession) -> None:

    with get_session() as session:
        suburb = upsert_suburb(session, suburb_name, state, city,
                               lga=_resolve_lga(suburb_name, city))

        for asset_type in ["house", "unit"]:
            price_cap = (cfg.price_caps.house_suburb_median_max if asset_type == "house"
                         else cfg.price_caps.unit_suburb_median_max)

            # --- domain.com.au suburb profile ---
            domain_stats = scrape_domain_suburb_profile(
                session_pw, suburb_name, state, asset_type
            )
            random_delay(3, 7)

            # --- REA neighbourhood (vacancy/rent) ---
            rea_stats = scrape_rea_suburb_profile(session_pw, suburb_name, state)
            random_delay(3, 7)

            # --- SQM Research ---
            sqm_stats = scrape_sqm_vacancy(suburb_name, state)

            # Merge: domain is primary for prices/clearance; REA/SQM for vacancy/rent
            median_price = domain_stats.get("median_price")
            if median_price and median_price > price_cap:
                logger.debug(f"Skip {suburb_name} {asset_type}: "
                             f"${median_price:,.0f} > cap ${price_cap:,.0f}")
                continue

            vacancy = (sqm_stats.get("vacancy_rate") or
                       rea_stats.get("vacancy_rate") or
                       domain_stats.get("vacancy_rate"))
            weekly_rent = (rea_stats.get("median_weekly_rent") or
                           domain_stats.get("median_weekly_rent"))

            # Derive yield if not given
            gross_yield = (rea_stats.get("gross_yield_pct") or
                           domain_stats.get("gross_yield_pct"))
            if not gross_yield and median_price and weekly_rent:
                gross_yield = (weekly_rent * 52) / median_price * 100

            clearance = domain_stats.get("auction_clearance_rate")
            clearance_avg = clearance  # single-month placeholder

            stock = (sqm_stats.get("stock_on_market") or
                     domain_stats.get("stock_on_market"))

            # Vacancy trend (compare to prior month if available)
            prior = (session.query(SuburbFundamentals)
                     .filter_by(suburb_id=suburb.id, asset_type=asset_type)
                     .order_by(SuburbFundamentals.snapshot_month.desc())
                     .first())
            prior_vacancy = prior.vacancy_rate if prior else None
            if prior_vacancy is not None and vacancy is not None:
                if vacancy > prior_vacancy + 0.2:
                    vacancy_trend = "rising"
                elif vacancy < prior_vacancy - 0.2:
                    vacancy_trend = "falling"
                else:
                    vacancy_trend = "stable"
            else:
                vacancy_trend = "stable"

            f = SuburbFundamentals(
                suburb_id=suburb.id,
                snapshot_month=snapshot_month,
                asset_type=asset_type,
                median_price=median_price,
                median_price_12m_ago=domain_stats.get("median_price_12m_ago"),
                price_momentum_pct=domain_stats.get("price_momentum_pct"),
                auction_clearance_rate=clearance,
                auction_clearance_12m_avg=clearance_avg,
                stock_on_market=stock,
                vacancy_rate=vacancy,
                vacancy_rate_prior=prior_vacancy,
                vacancy_trend=vacancy_trend,
                median_weekly_rent=weekly_rent,
                rent_growth_12m_pct=rea_stats.get("rent_growth_12m_pct"),
                gross_yield_pct=gross_yield,
            )

            existing = (session.query(SuburbFundamentals)
                        .filter_by(suburb_id=suburb.id,
                                   snapshot_month=snapshot_month,
                                   asset_type=asset_type)
                        .first())
            if existing:
                for attr, val in vars(f).items():
                    if not attr.startswith("_") and attr not in ("id",) and val is not None:
                        setattr(existing, attr, val)
            else:
                session.add(f)

    logger.debug(f"Processed {suburb_name} ({city})")


def run_fundamentals_scrape(cfg: Config) -> None:
    snapshot_month = datetime.utcnow().strftime("%Y-%m")
    logger.info(f"Starting fundamentals scrape for {snapshot_month} (Playwright)")

    suburb_lists = {"sydney": SYDNEY_SUBURBS, "melbourne": MELBOURNE_SUBURBS}

    with PlaywrightSession() as pw:
        for city in cfg.cities:
            state = CITY_STATES[city]
            suburbs = suburb_lists.get(city, [])
            logger.info(f"Processing {len(suburbs)} suburbs for {city}")

            for suburb_name, suburb_state in suburbs:
                logger.info(f"  Fundamentals: {suburb_name} ({city})")
                try:
                    _process_suburb(cfg, suburb_name, suburb_state, city,
                                    snapshot_month, pw)
                except Exception as e:
                    logger.error(f"Error processing {suburb_name}: {e}")
                random_delay(3, 8)

    logger.info("Fundamentals scrape complete")
