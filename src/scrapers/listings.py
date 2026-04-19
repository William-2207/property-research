"""
Weekly listings scraper — domain.com.au and realestate.com.au via Playwright.
Hard filter: list price <= $1,200,000.
Computes vendor motivation on ingest.
Raw HTML archived to ./data/raw_html/ for debugging.
"""
from __future__ import annotations
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from loguru import logger

from ..config import Config
from ..database.session import get_session, upsert_suburb
from ..database.models import Listing, ListingPriceHistory, SuburbFundamentals
from .playwright_base import PlaywrightSession, random_delay, pick_user_agent
from .suburb_list import SYDNEY_SUBURBS, MELBOURNE_SUBURBS

MAX_PAGES = 10
CITY_STATES = {"sydney": "NSW", "melbourne": "VIC"}

PROPERTY_TYPE_MAP = {
    "house": "house", "semi-detached": "house", "terrace": "house",
    "villa": "house", "duplex": "house", "acreage": "house",
    "apartment": "unit", "unit": "unit", "flat": "unit",
    "studio": "unit", "townhouse": "unit", "town house": "unit",
    "apartmentunitflat": "unit",
}


def _classify_asset(raw: str) -> str:
    return PROPERTY_TYPE_MAP.get(raw.lower().strip(), "house")


def _parse_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", str(text))
    try:
        v = float(cleaned)
        return v if v >= 80_000 else None
    except ValueError:
        return None


def _parse_price_range(text: str) -> Optional[float]:
    """For price ranges like '$800k - $900k', return the midpoint."""
    text = text.replace(",", "").upper()
    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3}
    nums = []
    for m in re.finditer(r"\$?([\d.]+)\s*([BMK]?)", text):
        val = float(m.group(1))
        mult = multipliers.get(m.group(2), 1)
        nums.append(val * mult)
    if not nums:
        return None
    return sum(nums) / len(nums)


def _extract_jsonld(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    results = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return results


def _extract_next_data(html: str) -> Optional[Dict]:
    """Extract __NEXT_DATA__ JSON embedded by Next.js apps (domain.com.au)."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def _extract_rea_state(html: str) -> Optional[Dict]:
    """Extract __NEXT_DATA__ or window.__data__ from REA pages."""
    for pattern in [
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'window\.__data__\s*=\s*(\{.*?\});\s*</script>',
        r'window\.__LISTING_DATA__\s*=\s*(\{.*?\});\s*</script>',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
    return None


def _get_suburb_avg_dom(session, suburb_id: int, asset_type: str) -> float:
    recent = (datetime.utcnow() - timedelta(days=45)).strftime("%Y-%m")
    row = (session.query(SuburbFundamentals)
           .filter(SuburbFundamentals.suburb_id == suburb_id,
                   SuburbFundamentals.asset_type == asset_type,
                   SuburbFundamentals.snapshot_month >= recent)
           .order_by(SuburbFundamentals.snapshot_month.desc())
           .first())
    return getattr(row, "days_on_market", None) or 45.0


def _vendor_motivation_score(price_drops: int, dom: int, avg_dom: float) -> float:
    score = min(price_drops * 2.0, 10.0)
    if avg_dom > 0:
        score += min((max(0, dom - avg_dom) / avg_dom) * 10, 10.0)
    return round(score, 2)


# ─── Domain.com.au scraper ────────────────────────────────────────────────────

DOMAIN_SEARCH_BASE = "https://www.domain.com.au/sale"

DOMAIN_ASSET_SLUGS = {
    "house": "house",
    "unit": "unit+apartment+townhouse",
}

DOMAIN_LISTING_SELECTORS = [
    "ul[data-testid='results'] li[data-testid='listing-card-wrapper-standard']",
    "ul.css-l6dy6 li",
    "div.listing-card",
    "article[data-testid]",
]

DOMAIN_WAIT_SELECTOR = "ul[data-testid='results'], div.listing-card"


def _build_domain_search_url(suburb: str, state: str, asset_type: str,
                              max_price: int, page: int) -> str:
    suburb_slug = suburb.lower().replace(" ", "-")
    state_lower = state.lower()
    prop_type = DOMAIN_ASSET_SLUGS[asset_type]
    return (f"{DOMAIN_SEARCH_BASE}/{suburb_slug}-{state_lower}/"
            f"?property-type={prop_type}&price=0-{max_price}&sort=price-asc&page={page}")


def _parse_domain_listing_card(card, suburb_name: str, state: str,
                                city: str, asset_type: str) -> Optional[Dict]:
    try:
        # Price
        price_el = (card.select_one("[data-testid='listing-card-price']") or
                    card.select_one("[class*='price']") or
                    card.select_one("p.css-mgq8yx"))
        price_text = price_el.get_text(strip=True) if price_el else ""
        price = _parse_price(price_text) or _parse_price_range(price_text)

        # Address
        addr_el = (card.select_one("span[itemprop='streetAddress']") or
                   card.select_one("[data-testid='listing-card-address']") or
                   card.select_one("h2[class*='address']") or
                   card.select_one("h2"))
        address = addr_el.get_text(strip=True) if addr_el else ""

        # Listing URL / ID
        link_el = card.select_one("a[href*='/property-']") or card.select_one("a[href]")
        href = link_el["href"] if link_el else ""
        if href.startswith("/"):
            href = "https://www.domain.com.au" + href
        external_id = f"domain_{re.sub(r'[^a-z0-9]', '_', href.split('/')[-2][:60])}"

        # Property type
        type_el = card.select_one("[data-testid='listing-card-property-type']")
        prop_type_raw = type_el.get_text(strip=True).lower() if type_el else asset_type
        actual_asset = _classify_asset(prop_type_raw)

        # Features
        beds = baths = cars = None
        for feat_el in card.select("[data-testid='property-features-text-container'] span,"
                                   " [class*='features'] li, [class*='property-features'] span"):
            t = feat_el.get_text(strip=True).lower()
            if "bed" in t or t.isdigit():
                m = re.search(r"(\d+)", t)
                if m and beds is None:
                    beds = int(m.group(1))
            elif "bath" in t:
                m = re.search(r"(\d+)", t)
                if m and baths is None:
                    baths = int(m.group(1))
            elif "car" in t or "park" in t or "garage" in t:
                m = re.search(r"(\d+)", t)
                if m and cars is None:
                    cars = int(m.group(1))

        # Area
        area_el = card.select_one("[data-testid='listing-card-area']")
        floor_area = land_area = None
        if area_el:
            area_text = area_el.get_text()
            nums = re.findall(r"[\d,]+", area_text.replace(",", ""))
            if len(nums) >= 2:
                floor_area = float(nums[0])
                land_area = float(nums[1])
            elif len(nums) == 1:
                land_area = float(nums[0])

        area_for_calc = floor_area or land_area
        price_per_sqm = (price / area_for_calc
                         if price and area_for_calc and area_for_calc > 0 else None)

        if not address and not price:
            return None

        return {
            "external_id": external_id,
            "source": "domain",
            "url": href,
            "address": address[:300],
            "suburb_name": suburb_name,
            "state": state,
            "city": city,
            "postcode": "",
            "lat": None, "lon": None,
            "property_type": prop_type_raw,
            "asset_type": actual_asset,
            "bedrooms": beds,
            "bathrooms": baths,
            "car_spaces": cars,
            "floor_area_sqm": floor_area,
            "land_size_sqm": land_area,
            "list_price": price,
            "price_per_sqm": price_per_sqm,
            "sale_method": "",
            "listing_agent": "",
            "days_on_market": 0,
            "price_drop_count": 0,
            "date_first_listed": None,
        }
    except Exception as e:
        logger.debug(f"Domain card parse error: {e}")
        return None


def _parse_domain_next_data(data: Dict, suburb_name: str, state: str,
                             city: str, asset_type: str,
                             max_price: int) -> List[Dict]:
    """Parse listings from __NEXT_DATA__ JSON if DOM parse yields nothing."""
    results = []
    try:
        props = data.get("props", {}).get("pageProps", {})
        listings_data = (props.get("componentProps", {}).get("listingsMap") or
                         props.get("listings") or
                         props.get("listingMap") or {})

        if isinstance(listings_data, dict):
            items = list(listings_data.values())
        elif isinstance(listings_data, list):
            items = listings_data
        else:
            return results

        for item in items:
            listing = item.get("listing", item)
            price_obj = listing.get("priceDetails", {})
            price_text = (str(price_obj.get("price", "")) or
                          price_obj.get("displayPrice", ""))
            price = _parse_price(price_text) or _parse_price_range(price_text)
            if not price or price > max_price:
                continue

            prop_obj = listing.get("propertyDetails", listing)
            prop_type_raw = prop_obj.get("propertyType", "House").lower()
            actual_asset = _classify_asset(prop_type_raw)

            listing_id = listing.get("id", listing.get("listingId", ""))
            url = (listing.get("listingUrl") or
                   f"https://www.domain.com.au/property-{listing_id}")

            addr_obj = prop_obj.get("address", {})
            address = (prop_obj.get("displayableAddress") or
                       addr_obj.get("displayAddress") or
                       " ".join(filter(None, [
                           addr_obj.get("streetNumber", ""),
                           addr_obj.get("street", ""),
                           addr_obj.get("suburb", suburb_name),
                       ])))

            floor_area = prop_obj.get("floorArea") or prop_obj.get("internalArea")
            land_area = prop_obj.get("landArea") or prop_obj.get("area")
            area = floor_area or land_area
            ppsqm = price / area if price and area and area > 0 else None

            results.append({
                "external_id": f"domain_{listing_id}",
                "source": "domain",
                "url": url,
                "address": str(address)[:300],
                "suburb_name": suburb_name,
                "state": state,
                "city": city,
                "postcode": str(addr_obj.get("postCode", "")),
                "lat": prop_obj.get("latitude"),
                "lon": prop_obj.get("longitude"),
                "property_type": prop_type_raw,
                "asset_type": actual_asset,
                "bedrooms": prop_obj.get("bedrooms"),
                "bathrooms": prop_obj.get("bathrooms"),
                "car_spaces": prop_obj.get("carspaces"),
                "floor_area_sqm": floor_area,
                "land_size_sqm": land_area,
                "list_price": price,
                "price_per_sqm": ppsqm,
                "sale_method": listing.get("saleMode", "").lower(),
                "listing_agent": "",
                "days_on_market": listing.get("daysListed", 0) or 0,
                "price_drop_count": 0,
                "date_first_listed": None,
            })
    except Exception as e:
        logger.debug(f"Domain __NEXT_DATA__ parse error: {e}")
    return results


def scrape_domain_suburb(session_pw: PlaywrightSession, suburb: str, state: str,
                         city: str, max_price: int, asset_type: str) -> List[Dict]:
    listings: List[Dict] = []
    seen_ids: set = set()

    for page_num in range(1, MAX_PAGES + 1):
        url = _build_domain_search_url(suburb, state, asset_type, max_price, page_num)
        label = f"domain_{suburb}_{state}_{asset_type}_p{page_num}"

        if page_num > 1:
            random_delay(3, 8)

        html = session_pw.fetch_page(url, "domain", label, DOMAIN_WAIT_SELECTOR)
        if not html:
            break

        # Try __NEXT_DATA__ first (most reliable)
        next_data = _extract_next_data(html)
        if next_data:
            page_listings = _parse_domain_next_data(
                next_data, suburb, state, city, asset_type, max_price
            )
            if page_listings:
                for l in page_listings:
                    if l["external_id"] not in seen_ids:
                        seen_ids.add(l["external_id"])
                        listings.append(l)
                logger.debug(f"Domain (JSON) {suburb} {asset_type} p{page_num}: "
                             f"{len(page_listings)} listings")
                if len(page_listings) < 20:
                    break
                continue

        # Fallback: parse DOM
        soup = BeautifulSoup(html, "lxml")
        cards = []
        for selector in DOMAIN_LISTING_SELECTORS:
            cards = soup.select(selector)
            if cards:
                break

        if not cards:
            logger.debug(f"Domain DOM: no cards found on {url}")
            break

        page_new = 0
        for card in cards:
            raw = _parse_domain_listing_card(card, suburb, state, city, asset_type)
            if raw and raw["external_id"] not in seen_ids:
                if raw.get("list_price") and raw["list_price"] <= max_price:
                    seen_ids.add(raw["external_id"])
                    listings.append(raw)
                    page_new += 1

        logger.debug(f"Domain (DOM) {suburb} {asset_type} p{page_num}: {page_new} new listings")
        if page_new < 5:
            break

    return listings


# ─── realestate.com.au scraper ────────────────────────────────────────────────

REA_SEARCH_BASE = "https://www.realestate.com.au/buy"

REA_ASSET_SLUGS = {
    "house":  "house",
    "unit":   "unit+apartment+townhouse",
}

REA_WAIT_SELECTOR = "[data-testid='listing-card'], div.residential-card"


def _build_rea_search_url(suburb: str, state: str, asset_type: str,
                           max_price: int, page: int) -> str:
    suburb_slug = suburb.lower().replace(" ", "-")
    state_lower = state.lower()
    prop_type = REA_ASSET_SLUGS[asset_type]
    return (f"{REA_SEARCH_BASE}/in-{suburb_slug},+{state_lower}/"
            f"list-{page}?maxprice={max_price}&property-type={prop_type}")


def _parse_rea_next_data(data: Dict, suburb_name: str, state: str,
                          city: str, asset_type: str, max_price: int) -> List[Dict]:
    results = []
    try:
        props = data.get("props", {}).get("pageProps", {})
        component_props = props.get("componentProps", props)
        listings_data = (component_props.get("listings") or
                         component_props.get("listingModels") or
                         props.get("listings") or [])

        if isinstance(listings_data, dict):
            listings_data = list(listings_data.values())

        for item in listings_data:
            price_obj = item.get("price", {})
            price_text = (str(price_obj.get("value", "")) or
                          price_obj.get("display", ""))
            price = _parse_price(price_text) or _parse_price_range(price_text)
            if not price or price > max_price:
                continue

            prop_type_obj = item.get("propertyType", {})
            prop_type_raw = (prop_type_obj.get("display", "house")
                             if isinstance(prop_type_obj, dict)
                             else str(prop_type_obj)).lower()
            actual_asset = _classify_asset(prop_type_raw)

            listing_id = item.get("id") or item.get("listingId") or ""
            address_obj = item.get("address", {})
            full_address = " ".join(filter(None, [
                address_obj.get("streetAddress", ""),
                address_obj.get("suburb", suburb_name),
                address_obj.get("state", state),
                address_obj.get("postcode", ""),
            ]))

            features = item.get("generalFeatures", {})
            if isinstance(features, dict):
                beds = (features.get("bedrooms", {}) or {}).get("value")
                baths = (features.get("bathrooms", {}) or {}).get("value")
                cars = (features.get("parkingSpaces", {}) or {}).get("value")
            else:
                beds = baths = cars = None

            floor_area = (item.get("floorArea", {}) or {}).get("value")
            land_area = (item.get("landArea", {}) or {}).get("value")
            area = floor_area or land_area
            ppsqm = price / area if price and area and area > 0 else None

            results.append({
                "external_id": f"rea_{listing_id}",
                "source": "rea",
                "url": item.get("listingUrl", f"https://www.realestate.com.au/{listing_id}"),
                "address": full_address[:300],
                "suburb_name": suburb_name,
                "state": state,
                "city": city,
                "postcode": address_obj.get("postcode", ""),
                "lat": item.get("latitude"),
                "lon": item.get("longitude"),
                "property_type": prop_type_raw,
                "asset_type": actual_asset,
                "bedrooms": beds,
                "bathrooms": baths,
                "car_spaces": cars,
                "floor_area_sqm": floor_area,
                "land_size_sqm": land_area,
                "list_price": price,
                "price_per_sqm": ppsqm,
                "sale_method": item.get("channel", "").lower(),
                "listing_agent": "",
                "days_on_market": item.get("daysListed") or 0,
                "price_drop_count": 0,
                "date_first_listed": None,
            })
    except Exception as e:
        logger.debug(f"REA __NEXT_DATA__ parse error: {e}")
    return results


def _parse_rea_listing_card(card, suburb_name: str, state: str,
                              city: str, asset_type: str) -> Optional[Dict]:
    try:
        price_el = (card.select_one("[data-testid='listing-card-price']") or
                    card.select_one("[class*='price']"))
        price_text = price_el.get_text(strip=True) if price_el else ""
        price = _parse_price(price_text) or _parse_price_range(price_text)

        addr_el = (card.select_one("[data-testid='address']") or
                   card.select_one("[class*='address']"))
        address = addr_el.get_text(strip=True) if addr_el else ""

        link_el = (card.select_one("a[href*='/property-']") or
                   card.select_one("a[href*='real']"))
        href = link_el["href"] if link_el else ""
        if href and not href.startswith("http"):
            href = "https://www.realestate.com.au" + href
        external_id = f"rea_{re.sub(r'[^a-z0-9]', '_', href[-50:])}"

        beds = baths = cars = None
        for span in card.select("span, li"):
            t = span.get_text(strip=True)
            if re.match(r"^\d+ Bed", t, re.I):
                beds = int(re.search(r"\d+", t).group())
            elif re.match(r"^\d+ Bath", t, re.I):
                baths = int(re.search(r"\d+", t).group())
            elif re.match(r"^\d+ (Car|Park|Garage)", t, re.I):
                cars = int(re.search(r"\d+", t).group())

        if not price and not address:
            return None

        return {
            "external_id": external_id,
            "source": "rea",
            "url": href,
            "address": address[:300],
            "suburb_name": suburb_name,
            "state": state,
            "city": city,
            "postcode": "",
            "lat": None, "lon": None,
            "property_type": asset_type,
            "asset_type": asset_type,
            "bedrooms": beds,
            "bathrooms": baths,
            "car_spaces": cars,
            "floor_area_sqm": None,
            "land_size_sqm": None,
            "list_price": price,
            "price_per_sqm": None,
            "sale_method": "",
            "listing_agent": "",
            "days_on_market": 0,
            "price_drop_count": 0,
            "date_first_listed": None,
        }
    except Exception as e:
        logger.debug(f"REA card parse error: {e}")
        return None


def scrape_rea_suburb(session_pw: PlaywrightSession, suburb: str, state: str,
                      city: str, max_price: int, asset_type: str) -> List[Dict]:
    listings: List[Dict] = []
    seen_ids: set = set()

    for page_num in range(1, MAX_PAGES + 1):
        url = _build_rea_search_url(suburb, state, asset_type, max_price, page_num)
        label = f"rea_{suburb}_{state}_{asset_type}_p{page_num}"

        if page_num > 1:
            random_delay(3, 8)

        html = session_pw.fetch_page(url, "rea", label, REA_WAIT_SELECTOR)
        if not html:
            break

        # Try __NEXT_DATA__
        rea_state = _extract_rea_state(html)
        if rea_state:
            page_listings = _parse_rea_next_data(
                rea_state, suburb, state, city, asset_type, max_price
            )
            if page_listings:
                for l in page_listings:
                    if l["external_id"] not in seen_ids:
                        seen_ids.add(l["external_id"])
                        listings.append(l)
                logger.debug(f"REA (JSON) {suburb} {asset_type} p{page_num}: "
                             f"{len(page_listings)} listings")
                if len(page_listings) < 20:
                    break
                continue

        # Fallback: DOM
        soup = BeautifulSoup(html, "lxml")
        cards = (soup.select("[data-testid='listing-card']") or
                 soup.select("div.residential-card") or
                 soup.select("li.residential-card") or
                 soup.select("[class*='ListingCard']"))

        if not cards:
            logger.debug(f"REA DOM: no cards on {url}")
            break

        page_new = 0
        for card in cards:
            raw = _parse_rea_listing_card(card, suburb, state, city, asset_type)
            if raw and raw["external_id"] not in seen_ids:
                if raw.get("list_price") and raw["list_price"] <= max_price:
                    seen_ids.add(raw["external_id"])
                    listings.append(raw)
                    page_new += 1

        logger.debug(f"REA (DOM) {suburb} {asset_type} p{page_num}: {page_new} new listings")
        if page_new < 5:
            break

    return listings


# ─── Ingest ───────────────────────────────────────────────────────────────────

def _upsert_listing(session, raw: Dict, suburb_id: int, avg_dom: float) -> None:
    existing = session.query(Listing).filter_by(external_id=raw["external_id"]).first()
    dom = raw.get("days_on_market", 0) or 0
    dom_vs_avg = dom / avg_dom if avg_dom > 0 else 1.0
    vendor_score = _vendor_motivation_score(raw.get("price_drop_count", 0), dom, avg_dom)

    if existing:
        new_price = raw.get("list_price")
        if new_price and existing.list_price and new_price < existing.list_price:
            session.add(ListingPriceHistory(
                listing_id=existing.id,
                price=existing.list_price,
                source=raw["source"],
            ))
            existing.price_drop_count = (existing.price_drop_count or 0) + 1
            existing.list_price = new_price
            existing.vendor_motivation_score = _vendor_motivation_score(
                existing.price_drop_count, dom, avg_dom
            )
        existing.days_on_market = dom
        existing.dom_vs_avg_ratio = dom_vs_avg
        existing.updated_at = datetime.utcnow()
        return

    session.add(Listing(
        suburb_id=suburb_id, city=raw["city"],
        external_id=raw["external_id"], source=raw["source"],
        url=raw.get("url", ""), address=raw.get("address", ""),
        suburb_name=raw.get("suburb_name", ""), state=raw.get("state", ""),
        postcode=raw.get("postcode", ""), lat=raw.get("lat"), lon=raw.get("lon"),
        property_type=raw.get("property_type", ""),
        asset_type=raw.get("asset_type", "house"),
        bedrooms=raw.get("bedrooms"), bathrooms=raw.get("bathrooms"),
        car_spaces=raw.get("car_spaces"),
        floor_area_sqm=raw.get("floor_area_sqm"),
        land_size_sqm=raw.get("land_size_sqm"),
        list_price=raw.get("list_price"),
        price_per_sqm=raw.get("price_per_sqm"),
        sale_method=raw.get("sale_method", ""),
        listing_agent=raw.get("listing_agent", ""),
        days_on_market=dom, suburb_avg_dom=avg_dom,
        dom_vs_avg_ratio=dom_vs_avg,
        price_drop_count=raw.get("price_drop_count", 0),
        vendor_motivation_score=vendor_score,
        date_first_listed=raw.get("date_first_listed"),
        date_last_updated=datetime.utcnow(),
    ))


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def run_listings_scrape(cfg: Config) -> None:
    logger.info("Starting weekly listings scrape (Playwright — Domain + REA)")

    suburb_lists = {"sydney": SYDNEY_SUBURBS, "melbourne": MELBOURNE_SUBURBS}

    with PlaywrightSession() as pw:
        for city in cfg.cities:
            state = CITY_STATES[city]
            suburbs = suburb_lists.get(city, [])
            logger.info(f"Scraping {len(suburbs)} suburbs for {city}")

            for suburb_name, suburb_state in suburbs:
                logger.info(f"  {suburb_name} ({city})")
                all_raw: List[Dict] = []

                # Domain
                for asset_type in ["house", "unit"]:
                    try:
                        got = scrape_domain_suburb(
                            pw, suburb_name, suburb_state, city,
                            cfg.price_caps.listing_max_price, asset_type
                        )
                        all_raw.extend(got)
                        logger.info(f"    Domain {asset_type}: {len(got)}")
                    except Exception as e:
                        logger.error(f"    Domain {asset_type} error: {e}")
                    random_delay(3, 8)

                # REA
                for asset_type in ["house", "unit"]:
                    try:
                        got = scrape_rea_suburb(
                            pw, suburb_name, suburb_state, city,
                            cfg.price_caps.listing_max_price, asset_type
                        )
                        # Deduplicate against domain results by address similarity
                        existing_addrs = {r["address"][:40] for r in all_raw}
                        new = [r for r in got
                               if r["address"][:40] not in existing_addrs]
                        all_raw.extend(new)
                        logger.info(f"    REA {asset_type}: {len(got)} (+{len(new)} new)")
                    except Exception as e:
                        logger.error(f"    REA {asset_type} error: {e}")
                    random_delay(3, 8)

                # Persist
                with get_session() as session:
                    suburb_obj = upsert_suburb(session, suburb_name, suburb_state, city)
                    ingested = 0
                    for raw in all_raw:
                        if raw.get("list_price", 0) > cfg.price_caps.listing_max_price:
                            continue
                        avg_dom = _get_suburb_avg_dom(
                            session, suburb_obj.id, raw.get("asset_type", "house")
                        )
                        try:
                            _upsert_listing(session, raw, suburb_obj.id, avg_dom)
                            ingested += 1
                        except Exception as e:
                            logger.debug(f"Upsert error {raw.get('external_id')}: {e}")
                    logger.info(f"    Persisted {ingested} listings for {suburb_name}")

    logger.info("Listings scrape complete")
