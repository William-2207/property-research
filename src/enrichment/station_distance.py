"""
Station distance enrichment using OpenStreetMap Overpass API.
Loads train/metro stations and computes walking distance to nearest station per listing.
"""
from __future__ import annotations
import math
import time
from typing import Dict, List, Optional, Tuple
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config, StationDistanceCfg
from ..database.session import get_session
from ..database.models import TrainStation, Listing

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

NSW_BBOX = (-34.5, 150.0, -32.5, 152.0)
VIC_BBOX = (-38.5, 143.5, -36.5, 146.5)

WALKING_FACTOR = 1.35


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def walking_distance_m(straight_line_m: float) -> float:
    return straight_line_m * WALKING_FACTOR


def categorise_station_distance(walking_m: float, cfg: StationDistanceCfg) -> str:
    if walking_m <= cfg.premium_max_m:
        return "Premium"
    if walking_m <= cfg.good_max_m:
        return "Good"
    if walking_m <= cfg.acceptable_max_m:
        return "Acceptable"
    return "Penalised"


def station_distance_penalty(walking_m: float, cfg: StationDistanceCfg) -> float:
    """Returns penalty percentage to subtract from undervalue score."""
    if walking_m <= cfg.penalty_start_m:
        return 0.0
    penalty = 0.0
    excess_beyond_800 = walking_m - cfg.penalty_start_m
    penalty += (excess_beyond_800 / 200) * cfg.penalty_per_200m
    if walking_m > cfg.acceptable_max_m:
        penalty += cfg.penalty_beyond_1200m_flat
    return round(penalty, 2)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=5, max=30))
def _fetch_stations_overpass(city: str) -> List[Dict]:
    bbox = NSW_BBOX if city == "sydney" else VIC_BBOX
    s, w, n, e = bbox
    query = f"""
[out:json][timeout:60];
(
  node["railway"="station"]({s},{w},{n},{e});
  node["railway"="halt"]({s},{w},{n},{e});
  node["railway"="subway_entrance"]({s},{w},{n},{e});
  node["public_transport"="station"]({s},{w},{n},{e});
  node["amenity"="ferry_terminal"]({s},{w},{n},{e});
);
out body;
"""
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=90)
    resp.raise_for_status()
    data = resp.json()
    stations = []
    for element in data.get("elements", []):
        if element.get("type") != "node":
            continue
        tags = element.get("tags", {})
        name = (tags.get("name") or tags.get("official_name") or
                tags.get("ref_name") or "Unknown Station")
        station_type = tags.get("railway", tags.get("public_transport", "train"))
        stations.append({
            "station_name": name,
            "lat": element["lat"],
            "lon": element["lon"],
            "station_type": station_type,
            "source": "osm",
        })
    return stations


def load_stations_for_city(cfg: Config, city: str) -> None:
    logger.info(f"Loading station data for {city} via Overpass API")
    try:
        stations = _fetch_stations_overpass(city)
        logger.info(f"Found {len(stations)} stations for {city}")

        with get_session() as session:
            existing_names = {
                s.station_name for s in session.query(TrainStation).filter_by(city=city).all()
            }
            new_count = 0
            for st in stations:
                if st["station_name"] not in existing_names:
                    station = TrainStation(
                        city=city,
                        station_name=st["station_name"],
                        lat=st["lat"],
                        lon=st["lon"],
                        station_type=st["station_type"],
                        source=st["source"],
                    )
                    session.add(station)
                    new_count += 1
            logger.info(f"Added {new_count} new stations for {city}")
    except Exception as e:
        logger.error(f"Failed to load stations for {city}: {e}")


def find_nearest_station(lat: float, lon: float, city: str,
                         session) -> Tuple[Optional[str], Optional[float]]:
    stations = session.query(TrainStation).filter_by(city=city).all()
    if not stations:
        return None, None

    best_name = None
    best_dist = float("inf")
    for st in stations:
        d = haversine_m(lat, lon, st.lat, st.lon)
        walking = walking_distance_m(d)
        if walking < best_dist:
            best_dist = walking
            best_name = st.station_name

    return best_name, round(best_dist, 1)


def enrich_listing_station(listing: Listing, cfg: Config, session) -> None:
    if not listing.lat or not listing.lon:
        return

    city = listing.city
    station_name, walking_m = find_nearest_station(listing.lat, listing.lon, city, session)
    if station_name is None:
        return

    listing.station_name = station_name
    listing.station_distance_m = walking_m
    listing.station_category = categorise_station_distance(walking_m, cfg.station_distance)


def geocode_address(address: str, suburb: str, state: str) -> Optional[Tuple[float, float]]:
    query = f"{address}, {suburb}, {state}, Australia"
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "au"},
            headers={"User-Agent": "PropertyResearchAgent/1.0 (wwiraatmadja@gmail.com)"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None


def run_station_enrichment(cfg: Config) -> None:
    logger.info("Running station distance enrichment")

    for city in cfg.cities:
        with get_session() as session:
            station_count = session.query(TrainStation).filter_by(city=city).count()
        if station_count == 0:
            load_stations_for_city(cfg, city)
            time.sleep(5)

    with get_session() as session:
        listings = session.query(Listing).filter(
            Listing.station_distance_m.is_(None),
            Listing.is_active == True,
        ).limit(500).all()

        logger.info(f"Enriching station distance for {len(listings)} listings")
        for listing in listings:
            if not listing.lat or not listing.lon:
                coords = geocode_address(
                    listing.address, listing.suburb_name, listing.state
                )
                if coords:
                    listing.lat, listing.lon = coords
                    time.sleep(1.1)
                else:
                    continue
            enrich_listing_station(listing, cfg, session)
