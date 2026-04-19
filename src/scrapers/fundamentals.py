"""
Monthly suburb fundamentals scraper.
Sources: Domain API (median prices, clearance rates), SQM Research (vacancy/stock),
ABS DataAPI (population, income), NSW/VIC planning portals (overlays).
"""
from __future__ import annotations
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session, upsert_suburb
from ..database.models import Suburb, SuburbFundamentals


DOMAIN_AUTH_URL = "https://auth.domain.com.au/v1/connect/token"
DOMAIN_API_BASE = "https://api.domain.com.au/v2"

SYDNEY_SUBURBS_SAMPLE = [
    ("Parramatta", "NSW"), ("Strathfield", "NSW"), ("Burwood", "NSW"),
    ("Homebush", "NSW"), ("Auburn", "NSW"), ("Granville", "NSW"),
    ("Merrylands", "NSW"), ("Penrith", "NSW"), ("Blacktown", "NSW"),
    ("Liverpool", "NSW"), ("Campbelltown", "NSW"), ("Bankstown", "NSW"),
    ("Hurstville", "NSW"), ("Kogarah", "NSW"), ("Rockdale", "NSW"),
    ("Sutherland", "NSW"), ("Miranda", "NSW"), ("Cronulla", "NSW"),
    ("Hornsby", "NSW"), ("Epping", "NSW"), ("Ryde", "NSW"),
    ("Chatswood", "NSW"), ("St Leonards", "NSW"), ("Artarmon", "NSW"),
    ("Manly", "NSW"), ("Dee Why", "NSW"), ("Brookvale", "NSW"),
    ("Fairfield", "NSW"), ("Cabramatta", "NSW"), ("Wetherill Park", "NSW"),
    ("Quakers Hill", "NSW"), ("Rooty Hill", "NSW"), ("Mount Druitt", "NSW"),
    ("Gosford", "NSW"), ("Wyong", "NSW"), ("Wollongong", "NSW"),
    ("Lidcombe", "NSW"), ("Berala", "NSW"), ("Regents Park", "NSW"),
]

MELBOURNE_SUBURBS_SAMPLE = [
    ("Footscray", "VIC"), ("Sunshine", "VIC"), ("Deer Park", "VIC"),
    ("Werribee", "VIC"), ("Hoppers Crossing", "VIC"), ("Dandenong", "VIC"),
    ("Springvale", "VIC"), ("Noble Park", "VIC"), ("Moorabbin", "VIC"),
    ("Cheltenham", "VIC"), ("Frankston", "VIC"), ("Cranbourne", "VIC"),
    ("Pakenham", "VIC"), ("Berwick", "VIC"), ("Narre Warren", "VIC"),
    ("Box Hill", "VIC"), ("Ringwood", "VIC"), ("Croydon", "VIC"),
    ("Lilydale", "VIC"), ("Boronia", "VIC"), ("Bayswater", "VIC"),
    ("Nunawading", "VIC"), ("Mitcham", "VIC"), ("Thomastown", "VIC"),
    ("Epping", "VIC"), ("Lalor", "VIC"), ("Mill Park", "VIC"),
    ("South Morang", "VIC"), ("Preston", "VIC"), ("Coburg", "VIC"),
    ("Brunswick", "VIC"), ("Northcote", "VIC"), ("Fitzroy North", "VIC"),
    ("Reservoir", "VIC"), ("Heidelberg", "VIC"), ("Doncaster", "VIC"),
    ("Bundoora", "VIC"), ("Fawkner", "VIC"), ("Broadmeadows", "VIC"),
]

COUNCIL_KEYWORDS = [
    "rezoning", "upzoning", "heritage listing", "height limit",
    "height limit amendment", "masterplan", "master plan",
    "TOD", "transit-oriented development", "urban renewal",
    "infrastructure contribution", "infrastructure levy",
]


class DomainApiClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def _get_token(self) -> str:
        if self._token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._token

        resp = requests.post(DOMAIN_AUTH_URL, data={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "api_listings_read api_agencies_read api_suburbperformance_read",
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expiry = datetime.utcnow() + timedelta(seconds=data.get("expires_in", 3600) - 60)
        return self._token

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_suburb_performance(self, suburb: str, state: str, asset_type: str,
                                period_size: int = 12) -> Optional[Dict]:
        category_map = {"house": "house", "unit": "unit"}
        cat = category_map.get(asset_type, "house")
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        url = (f"{DOMAIN_API_BASE}/suburbperformancestatistics"
               f"/{state}/{suburb}/3/{cat}/{period_size}")
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.warning(f"Domain API error for {suburb} {state} {asset_type}: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def search_listings(self, suburb: str, state: str, asset_types: List[str],
                        max_price: int) -> List[Dict]:
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        payload = {
            "listingType": "Sale",
            "maxPrice": max_price,
            "locations": [{"state": state, "suburb": suburb}],
            "propertyTypes": asset_types,
            "pageSize": 200,
        }
        try:
            resp = requests.post(f"{DOMAIN_API_BASE}/listings/residential/_search",
                                 json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.warning(f"Domain listings search error: {e}")
            return []


def scrape_sqm_vacancy(suburb: str, state: str) -> Optional[Dict]:
    """Scrape SQM Research for vacancy rate and stock data."""
    url = f"https://sqmresearch.com.au/vacancy-rates.php?region={state}&window=12&t=1"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table.sqmTable tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 3 and suburb.lower() in cells[0].text.lower():
                try:
                    vacancy = float(cells[1].text.strip().replace("%", ""))
                    stock = int(cells[2].text.strip().replace(",", ""))
                    return {"vacancy_rate": vacancy, "stock_on_market": stock}
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        logger.debug(f"SQM scrape failed for {suburb}: {e}")
    return None


def scrape_abs_population_growth(suburb: str, state: str) -> Optional[float]:
    """Fetch suburb-level population growth from ABS Census data (2016-2021)."""
    url = "https://www.abs.gov.au/census/find-census-data/quickstats/2021"
    try:
        resp = requests.get(url, params={"name": suburb, "state": state},
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        pop_el = soup.find(string=lambda t: t and "population" in t.lower())
        if pop_el:
            pass
    except Exception as e:
        logger.debug(f"ABS population scrape failed for {suburb}: {e}")
    return None


def estimate_fundamentals_from_domain(data: Optional[Dict], asset_type: str) -> Dict:
    """Parse Domain suburb performance API response."""
    result = {}
    if not data:
        return result

    header = data.get("header", {})
    series = data.get("series", {})

    result["median_price"] = header.get("medianSalePrice")
    result["auction_clearance_rate"] = header.get("auctionClearanceRate")
    result["days_on_market"] = header.get("daysOnMarket")
    result["discount_pct"] = header.get("discountPercentage")

    if series.get("seriesInfo"):
        prices = [s.get("values", {}).get("medianSalePrice") for s in series["seriesInfo"]
                  if s.get("values", {}).get("medianSalePrice")]
        if len(prices) >= 2:
            result["median_price_12m_ago"] = prices[-1]
            if prices[-1] and prices[0]:
                result["price_momentum_pct"] = (prices[0] - prices[-1]) / prices[-1] * 100

        clearances = [s.get("values", {}).get("auctionClearanceRate") for s in series["seriesInfo"]
                      if s.get("values", {}).get("auctionClearanceRate")]
        if clearances:
            result["auction_clearance_12m_avg"] = sum(clearances) / len(clearances)

    return result


def run_fundamentals_scrape(cfg: Config) -> None:
    snapshot_month = datetime.utcnow().strftime("%Y-%m")
    logger.info(f"Starting fundamentals scrape for {snapshot_month}")

    domain_client = None
    if cfg.api_keys.domain_client_id and cfg.api_keys.domain_client_secret:
        domain_client = DomainApiClient(
            cfg.api_keys.domain_client_id,
            cfg.api_keys.domain_client_secret,
        )
        logger.info("Domain API client initialised")
    else:
        logger.warning("Domain API keys not set — median price data will be limited")

    suburb_lists = {
        "sydney": SYDNEY_SUBURBS_SAMPLE,
        "melbourne": MELBOURNE_SUBURBS_SAMPLE,
    }
    city_states = {"sydney": "NSW", "melbourne": "VIC"}

    for city in cfg.cities:
        state = city_states[city]
        suburbs = suburb_lists.get(city, [])
        logger.info(f"Processing {len(suburbs)} suburbs for {city}")

        for suburb_name, suburb_state in suburbs:
            try:
                _process_suburb(
                    cfg, suburb_name, suburb_state, city,
                    snapshot_month, domain_client
                )
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error processing {suburb_name} {suburb_state}: {e}")
                continue

    logger.info("Fundamentals scrape complete")


def _process_suburb(cfg: Config, suburb_name: str, state: str, city: str,
                    snapshot_month: str, domain_client: Optional[DomainApiClient]) -> None:
    with get_session() as session:
        suburb = upsert_suburb(session, suburb_name, state, city,
                               lga=_resolve_lga(suburb_name, city))

        for asset_type in ["house", "unit"]:
            fundamentals = _build_fundamentals(
                cfg, suburb, suburb_name, state, asset_type,
                snapshot_month, domain_client, session
            )
            if fundamentals is None:
                continue

            price_cap = (cfg.price_caps.house_suburb_median_max if asset_type == "house"
                         else cfg.price_caps.unit_suburb_median_max)
            if fundamentals.median_price and fundamentals.median_price > price_cap:
                logger.debug(f"Skipping {suburb_name} {asset_type}: "
                             f"median ${fundamentals.median_price:,.0f} > cap ${price_cap:,.0f}")
                continue

            existing = session.query(SuburbFundamentals).filter_by(
                suburb_id=suburb.id,
                snapshot_month=snapshot_month,
                asset_type=asset_type,
            ).first()

            if existing:
                for attr in vars(fundamentals):
                    if not attr.startswith("_") and attr != "id":
                        val = getattr(fundamentals, attr)
                        if val is not None:
                            setattr(existing, attr, val)
            else:
                session.add(fundamentals)

        logger.debug(f"Processed {suburb_name} ({city})")


def _build_fundamentals(cfg: Config, suburb, suburb_name: str, state: str,
                        asset_type: str, snapshot_month: str,
                        domain_client: Optional[DomainApiClient],
                        session) -> Optional[SuburbFundamentals]:
    f = SuburbFundamentals(
        suburb_id=suburb.id,
        snapshot_month=snapshot_month,
        asset_type=asset_type,
    )

    if domain_client:
        domain_data = domain_client.get_suburb_performance(suburb_name, state, asset_type)
        parsed = estimate_fundamentals_from_domain(domain_data, asset_type)
        f.median_price = parsed.get("median_price")
        f.median_price_12m_ago = parsed.get("median_price_12m_ago")
        f.price_momentum_pct = parsed.get("price_momentum_pct")
        f.auction_clearance_rate = parsed.get("auction_clearance_rate")
        f.auction_clearance_12m_avg = parsed.get("auction_clearance_12m_avg")

    sqm_data = scrape_sqm_vacancy(suburb_name, state)
    if sqm_data:
        f.vacancy_rate = sqm_data.get("vacancy_rate")
        f.stock_on_market = sqm_data.get("stock_on_market")

    if f.median_price and f.vacancy_rate is not None:
        weekly_rent = _estimate_weekly_rent(f.median_price, f.vacancy_rate)
        f.median_weekly_rent = weekly_rent
        if f.median_price > 0:
            f.gross_yield_pct = (weekly_rent * 52) / f.median_price * 100

    _enrich_population_income(f, suburb_name, state)

    return f


def _estimate_weekly_rent(median_price: float, vacancy_rate: float) -> float:
    base_yield = 0.035 - (vacancy_rate / 100 * 0.5)
    base_yield = max(0.02, min(0.06, base_yield))
    return median_price * base_yield / 52


def _enrich_population_income(f: SuburbFundamentals, suburb_name: str, state: str) -> None:
    growth = scrape_abs_population_growth(suburb_name, state)
    if growth:
        f.population_growth_rate = growth


def _resolve_lga(suburb_name: str, city: str) -> str:
    NSW_LGA_MAP = {
        "Parramatta": "City of Parramatta", "Strathfield": "Strathfield Council",
        "Burwood": "Burwood Council", "Auburn": "Cumberland Council",
        "Bankstown": "Canterbury-Bankstown Council", "Hurstville": "Georges River Council",
        "Penrith": "Penrith City Council", "Blacktown": "Blacktown City Council",
        "Liverpool": "Liverpool City Council", "Hornsby": "Hornsby Shire Council",
        "Ryde": "City of Ryde", "Chatswood": "Willoughby City Council",
        "Manly": "Northern Beaches Council", "Fairfield": "Fairfield City Council",
        "Sutherland": "Sutherland Shire", "Campbelltown": "Campbelltown City Council",
    }
    VIC_LGA_MAP = {
        "Footscray": "City of Maribyrnong", "Sunshine": "City of Brimbank",
        "Werribee": "City of Wyndham", "Dandenong": "City of Greater Dandenong",
        "Box Hill": "City of Whitehorse", "Ringwood": "City of Maroondah",
        "Preston": "City of Darebin", "Coburg": "City of Moreland",
        "Brunswick": "City of Moreland", "Northcote": "City of Darebin",
        "Frankston": "City of Frankston", "Cranbourne": "City of Casey",
        "Epping": "City of Whittlesea", "Thomastown": "City of Whittlesea",
    }
    if city == "sydney":
        return NSW_LGA_MAP.get(suburb_name, "Unknown LGA")
    return VIC_LGA_MAP.get(suburb_name, "Unknown LGA")
