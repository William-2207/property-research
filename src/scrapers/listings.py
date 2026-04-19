"""
Weekly listings scraper — Domain.com.au (API) and realestate.com.au (Playwright).
Hard filters: list price <= $1,200,000. Computes vendor motivation on ingest.
"""
from __future__ import annotations
import json
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session, upsert_suburb
from ..database.models import Listing, ListingPriceHistory, Suburb, SuburbFundamentals
from .fundamentals import DomainApiClient, SYDNEY_SUBURBS_SAMPLE, MELBOURNE_SUBURBS_SAMPLE

PROPERTY_TYPE_MAP = {
    "house": "house",
    "semi-detached": "house",
    "terrace": "house",
    "villa": "house",
    "duplex": "house",
    "apartment": "unit",
    "unit": "unit",
    "flat": "unit",
    "studio": "unit",
    "townhouse": "unit",
    "town house": "unit",
}


def classify_asset_type(property_type: str) -> str:
    normalized = property_type.lower().strip()
    return PROPERTY_TYPE_MAP.get(normalized, "house")


def _extract_price(price_str: Optional[str]) -> Optional[float]:
    if not price_str:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(price_str))
    try:
        val = float(cleaned)
        return val if val > 50000 else None
    except ValueError:
        return None


def _compute_vendor_motivation(price_drop_count: int, dom: int, suburb_avg_dom: float) -> float:
    score = 0.0
    score += min(price_drop_count * 2.0, 10.0)
    if suburb_avg_dom and suburb_avg_dom > 0:
        excess_days = max(0, dom - suburb_avg_dom)
        score += min(excess_days / suburb_avg_dom * 10, 10.0)
    return round(score, 2)


def _get_suburb_avg_dom(session, suburb_id: int, asset_type: str) -> float:
    recent_month = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m")
    row = session.query(SuburbFundamentals).filter(
        SuburbFundamentals.suburb_id == suburb_id,
        SuburbFundamentals.asset_type == asset_type,
        SuburbFundamentals.snapshot_month >= recent_month,
    ).order_by(SuburbFundamentals.snapshot_month.desc()).first()
    return row.days_on_market if row and hasattr(row, 'days_on_market') and row.days_on_market else 45.0


class DomainListingsScraper:
    def __init__(self, client: DomainApiClient, cfg: Config):
        self.client = client
        self.cfg = cfg

    def scrape_suburb(self, suburb_name: str, state: str, city: str) -> List[Dict]:
        listings = []
        for prop_types, asset_type in [
            (["House", "Semi-Detached", "Terrace", "Villa", "Duplex"], "house"),
            (["ApartmentUnitFlat", "Townhouse", "Studio"], "unit"),
        ]:
            raw = self.client.search_listings(
                suburb_name, state, prop_types, self.cfg.price_caps.listing_max_price
            )
            for item in raw:
                listing = self._parse_domain_listing(item, suburb_name, state, city, asset_type)
                if listing:
                    listings.append(listing)
        return listings

    def _parse_domain_listing(self, item: Dict, suburb_name: str, state: str,
                               city: str, asset_type: str) -> Optional[Dict]:
        listing_id = item.get("id")
        if not listing_id:
            return None

        listing_details = item.get("listing", {})
        price_details = listing_details.get("priceDetails", {})
        property_details = listing_details.get("propertyDetails", {})

        price = _extract_price(price_details.get("price"))
        display_price = price_details.get("displayPrice", "")
        if price is None:
            price = _extract_price(re.sub(r"[^\d]", "", display_price))

        if price and price > self.cfg.price_caps.listing_max_price:
            return None

        prop_type_raw = property_details.get("propertyType", "House")
        prop_type = prop_type_raw.lower().replace("apartmentunitflat", "apartment")
        actual_asset = classify_asset_type(prop_type)

        floor_area = property_details.get("floorArea")
        land_area = property_details.get("landArea")
        price_per_sqm = None
        area_for_calc = floor_area or land_area
        if price and area_for_calc and area_for_calc > 0:
            price_per_sqm = price / area_for_calc

        date_listed_str = listing_details.get("dateListed")
        date_listed = None
        if date_listed_str:
            try:
                date_listed = datetime.fromisoformat(date_listed_str.replace("Z", "+00:00"))
            except ValueError:
                pass

        dom = listing_details.get("daysListed", 0) or 0

        return {
            "external_id": f"domain_{listing_id}",
            "source": "domain",
            "url": f"https://www.domain.com.au/{listing_id}",
            "address": property_details.get("displayableAddress", ""),
            "suburb_name": suburb_name,
            "state": state,
            "city": city,
            "postcode": property_details.get("postCode", ""),
            "lat": property_details.get("latitude"),
            "lon": property_details.get("longitude"),
            "property_type": prop_type,
            "asset_type": actual_asset,
            "bedrooms": property_details.get("bedrooms"),
            "bathrooms": property_details.get("bathrooms"),
            "car_spaces": property_details.get("carspaces"),
            "floor_area_sqm": floor_area,
            "land_size_sqm": land_area,
            "list_price": price,
            "price_per_sqm": price_per_sqm,
            "sale_method": listing_details.get("saleMode", "").lower(),
            "listing_agent": _extract_agent_name(listing_details.get("advertiser", {})),
            "days_on_market": dom,
            "price_drop_count": 0,
            "date_first_listed": date_listed,
        }


def _extract_agent_name(advertiser: Dict) -> str:
    if not advertiser:
        return ""
    name = advertiser.get("name", "")
    return name[:200]


class REAListingsScraper:
    BASE_URL = "https://www.realestate.com.au"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-AU,en;q=0.9",
        })

    def scrape_suburb(self, suburb_name: str, state: str, city: str) -> List[Dict]:
        listings = []
        for asset_type, search_type in [("house", "house"), ("unit", "unit+apartment+townhouse")]:
            suburb_slug = suburb_name.lower().replace(" ", "-")
            state_lower = state.lower()
            page = 1
            while True:
                url = (f"{self.BASE_URL}/buy/in-{suburb_slug},+{state_lower}/"
                       f"list-{page}?propertyTypes={search_type}"
                       f"&maxPrice={self.cfg.price_caps.listing_max_price}")
                try:
                    resp = self.session.get(url, timeout=20)
                    if resp.status_code != 200:
                        break
                    page_listings = self._parse_rea_page(resp.text, suburb_name, state, city, asset_type)
                    if not page_listings:
                        break
                    listings.extend(page_listings)
                    if len(page_listings) < 20 or page >= 5:
                        break
                    page += 1
                    time.sleep(1.5)
                except Exception as e:
                    logger.debug(f"REA scrape error {suburb_name} page {page}: {e}")
                    break
        return listings

    def _parse_rea_page(self, html: str, suburb_name: str, state: str,
                        city: str, asset_type: str) -> List[Dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []

        script_tags = soup.find_all("script", {"type": "application/json"})
        for script in script_tags:
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict) and "props" in data:
                    listings_data = (data.get("props", {})
                                     .get("pageProps", {})
                                     .get("componentProps", {})
                                     .get("listings", []))
                    for item in listings_data:
                        parsed = self._parse_rea_listing(item, suburb_name, state, city, asset_type)
                        if parsed:
                            results.append(parsed)
                    if results:
                        return results
            except (json.JSONDecodeError, AttributeError):
                continue

        cards = soup.select("[data-testid='listing-card']")
        for card in cards:
            parsed = self._parse_rea_card(card, suburb_name, state, city, asset_type)
            if parsed:
                results.append(parsed)

        return results

    def _parse_rea_listing(self, item: Dict, suburb_name: str, state: str,
                            city: str, asset_type: str) -> Optional[Dict]:
        listing_id = item.get("id") or item.get("listingId")
        if not listing_id:
            return None

        price = _extract_price(item.get("price", {}).get("display", ""))
        if not price:
            price = _extract_price(item.get("price", {}).get("value"))

        if price and price > self.cfg.price_caps.listing_max_price:
            return None

        features = item.get("generalFeatures", {})
        prop_type = item.get("propertyType", {}).get("display", "house").lower()
        actual_asset = classify_asset_type(prop_type)

        floor_area = item.get("floorArea", {}).get("value")
        land_area = item.get("landArea", {}).get("value")
        area_for_calc = floor_area or land_area
        price_per_sqm = (price / area_for_calc) if price and area_for_calc and area_for_calc > 0 else None

        address = item.get("address", {})
        full_address = " ".join(filter(None, [
            address.get("streetAddress", ""),
            address.get("suburb", suburb_name),
            address.get("state", state),
            address.get("postcode", ""),
        ]))

        return {
            "external_id": f"rea_{listing_id}",
            "source": "rea",
            "url": f"https://www.realestate.com.au/{listing_id}",
            "address": full_address[:300],
            "suburb_name": suburb_name,
            "state": state,
            "city": city,
            "postcode": address.get("postcode", ""),
            "lat": item.get("latitude"),
            "lon": item.get("longitude"),
            "property_type": prop_type,
            "asset_type": actual_asset,
            "bedrooms": features.get("bedrooms", {}).get("value"),
            "bathrooms": features.get("bathrooms", {}).get("value"),
            "car_spaces": features.get("parkingSpaces", {}).get("value"),
            "floor_area_sqm": floor_area,
            "land_size_sqm": land_area,
            "list_price": price,
            "price_per_sqm": price_per_sqm,
            "sale_method": item.get("channel", "").lower(),
            "listing_agent": "",
            "days_on_market": item.get("daysListed", 0) or 0,
            "price_drop_count": 0,
            "date_first_listed": None,
        }

    def _parse_rea_card(self, card, suburb_name: str, state: str,
                         city: str, asset_type: str) -> Optional[Dict]:
        try:
            price_el = card.select_one("[data-testid='listing-card-price']")
            price = _extract_price(price_el.text if price_el else "")
            if price and price > self.cfg.price_caps.listing_max_price:
                return None

            address_el = card.select_one("[data-testid='address']")
            address = address_el.text.strip() if address_el else ""

            link_el = card.select_one("a[href*='/property-']")
            listing_url = f"{self.BASE_URL}{link_el['href']}" if link_el else ""
            external_id = f"rea_{re.sub(r'[^a-z0-9]', '_', listing_url[-50:])}"

            beds_el = card.select_one("[data-testid='property-features-text-container']")
            beds = bathrooms = cars = None
            if beds_el:
                features_text = beds_el.text
                bed_match = re.search(r"(\d+)\s*Bed", features_text, re.I)
                bath_match = re.search(r"(\d+)\s*Bath", features_text, re.I)
                car_match = re.search(r"(\d+)\s*Car", features_text, re.I)
                if bed_match:
                    beds = int(bed_match.group(1))
                if bath_match:
                    bathrooms = int(bath_match.group(1))
                if car_match:
                    cars = int(car_match.group(1))

            return {
                "external_id": external_id,
                "source": "rea",
                "url": listing_url,
                "address": address[:300],
                "suburb_name": suburb_name,
                "state": state,
                "city": city,
                "postcode": "",
                "lat": None,
                "lon": None,
                "property_type": asset_type,
                "asset_type": asset_type,
                "bedrooms": beds,
                "bathrooms": bathrooms,
                "car_spaces": cars,
                "floor_area_sqm": None,
                "land_size_sqm": None,
                "list_price": price,
                "price_per_sqm": None,
                "sale_method": "private treaty",
                "listing_agent": "",
                "days_on_market": 0,
                "price_drop_count": 0,
                "date_first_listed": None,
            }
        except Exception as e:
            logger.debug(f"REA card parse error: {e}")
            return None


def upsert_listing(session, raw: Dict, suburb_id: int, suburb_avg_dom: float) -> Listing:
    existing = session.query(Listing).filter_by(external_id=raw["external_id"]).first()

    dom = raw.get("days_on_market", 0) or 0
    dom_vs_avg = dom / suburb_avg_dom if suburb_avg_dom > 0 else 1.0
    vendor_score = _compute_vendor_motivation(
        raw.get("price_drop_count", 0), dom, suburb_avg_dom
    )

    if existing:
        if raw.get("list_price") and raw["list_price"] != existing.list_price:
            history = ListingPriceHistory(
                listing_id=existing.id,
                price=raw["list_price"],
                source=raw["source"],
            )
            session.add(history)
            existing.price_drop_count = (existing.price_drop_count or 0) + 1
            existing.vendor_motivation_score = _compute_vendor_motivation(
                existing.price_drop_count, dom, suburb_avg_dom
            )
        existing.days_on_market = dom
        existing.dom_vs_avg_ratio = dom_vs_avg
        existing.updated_at = datetime.utcnow()
        return existing

    listing = Listing(
        suburb_id=suburb_id,
        city=raw["city"],
        external_id=raw["external_id"],
        source=raw["source"],
        url=raw.get("url", ""),
        address=raw.get("address", ""),
        suburb_name=raw.get("suburb_name", ""),
        state=raw.get("state", ""),
        postcode=raw.get("postcode", ""),
        lat=raw.get("lat"),
        lon=raw.get("lon"),
        property_type=raw.get("property_type", ""),
        asset_type=raw.get("asset_type", "house"),
        bedrooms=raw.get("bedrooms"),
        bathrooms=raw.get("bathrooms"),
        car_spaces=raw.get("car_spaces"),
        floor_area_sqm=raw.get("floor_area_sqm"),
        land_size_sqm=raw.get("land_size_sqm"),
        list_price=raw.get("list_price"),
        price_per_sqm=raw.get("price_per_sqm"),
        sale_method=raw.get("sale_method", ""),
        listing_agent=raw.get("listing_agent", ""),
        days_on_market=dom,
        suburb_avg_dom=suburb_avg_dom,
        dom_vs_avg_ratio=dom_vs_avg,
        price_drop_count=raw.get("price_drop_count", 0),
        vendor_motivation_score=vendor_score,
        date_first_listed=raw.get("date_first_listed"),
        date_last_updated=datetime.utcnow(),
    )
    session.add(listing)
    return listing


def run_listings_scrape(cfg: Config) -> None:
    logger.info("Starting weekly listings scrape")

    domain_client = None
    if cfg.api_keys.domain_client_id and cfg.api_keys.domain_client_secret:
        domain_client = DomainApiClient(
            cfg.api_keys.domain_client_id,
            cfg.api_keys.domain_client_secret,
        )

    domain_scraper = DomainListingsScraper(domain_client, cfg) if domain_client else None
    rea_scraper = REAListingsScraper(cfg)

    suburb_lists = {
        "sydney": SYDNEY_SUBURBS_SAMPLE,
        "melbourne": MELBOURNE_SUBURBS_SAMPLE,
    }
    city_states = {"sydney": "NSW", "melbourne": "VIC"}

    for city in cfg.cities:
        state = city_states[city]
        suburbs = suburb_lists.get(city, [])
        logger.info(f"Scraping listings for {len(suburbs)} suburbs in {city}")

        for suburb_name, suburb_state in suburbs:
            logger.debug(f"Scraping {suburb_name}")
            all_listings: List[Dict] = []

            if domain_scraper:
                try:
                    dl = domain_scraper.scrape_suburb(suburb_name, suburb_state, city)
                    all_listings.extend(dl)
                    logger.debug(f"Domain: {len(dl)} listings for {suburb_name}")
                except Exception as e:
                    logger.error(f"Domain scrape error {suburb_name}: {e}")

            try:
                rl = rea_scraper.scrape_suburb(suburb_name, suburb_state, city)
                seen_ids = {l["external_id"] for l in all_listings}
                rl_new = [l for l in rl if l["external_id"] not in seen_ids]
                all_listings.extend(rl_new)
                logger.debug(f"REA: {len(rl_new)} new listings for {suburb_name}")
            except Exception as e:
                logger.error(f"REA scrape error {suburb_name}: {e}")

            if not all_listings:
                time.sleep(0.5)
                continue

            with get_session() as session:
                suburb_obj = upsert_suburb(session, suburb_name, suburb_state, city)
                for asset_type in ["house", "unit"]:
                    avg_dom = _get_suburb_avg_dom(session, suburb_obj.id, asset_type)

                    type_listings = [l for l in all_listings if l.get("asset_type") == asset_type]
                    for raw in type_listings:
                        if raw.get("list_price") and raw["list_price"] > cfg.price_caps.listing_max_price:
                            continue
                        try:
                            upsert_listing(session, raw, suburb_obj.id, avg_dom)
                        except Exception as e:
                            logger.error(f"Upsert listing error {raw.get('external_id')}: {e}")

            time.sleep(1.0)

    logger.info("Listings scrape complete")
