"""
Planning overlay lookup — flood, bushfire, heritage, flight path.
Uses NSW Planning Portal and VIC Planning Portal APIs/pages.
Enriches both suburb-level and listing-level.
"""
from __future__ import annotations
import time
from typing import Dict, Optional, Tuple
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session
from ..database.models import Listing, SuburbFundamentals

NSW_PORTAL_BASE = "https://api.apps1.nsw.gov.au/planning/viewersf/V1/ePlanningApi/layer"
VIC_PORTAL_BASE = "https://services6.arcgis.com/GB33F62SbDxJjwEL/arcgis/rest/services"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

FLIGHT_PATH_RADIUS_KM = {
    "sydney": {"lat": -33.9399, "lon": 151.1753, "radius_km": 15},
    "melbourne": {"lat": -37.6690, "lon": 144.8410, "radius_km": 15},
}


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _check_nsw_overlay(lat: float, lon: float, overlay_type: str) -> bool:
    layer_map = {
        "flood": "floodplanning",
        "bushfire": "bfpzone",
        "heritage": "heritage",
    }
    layer = layer_map.get(overlay_type)
    if not layer:
        return False

    try:
        params = {
            "layerName": layer,
            "point": f"{lon},{lat}",
        }
        resp = requests.get(NSW_PORTAL_BASE, params=params, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return False
        data = resp.json()
        features = data.get("features", data.get("results", []))
        return len(features) > 0
    except Exception as e:
        logger.debug(f"NSW overlay check failed ({overlay_type}): {e}")
        return False


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _check_vic_overlay(lat: float, lon: float, overlay_type: str) -> bool:
    service_map = {
        "flood": "FloodOverlay_WFL1/FeatureServer/0",
        "bushfire": "BushfireManagementOverlay_WFL1/FeatureServer/0",
        "heritage": "HeritageOverlay_WFL1/FeatureServer/0",
    }
    service = service_map.get(overlay_type)
    if not service:
        return False

    try:
        geometry = {"x": lon, "y": lat, "spatialReference": {"wkid": 4326}}
        params = {
            "geometry": str(geometry).replace("'", '"'),
            "geometryType": "esriGeometryPoint",
            "spatialRel": "esriSpatialRelIntersects",
            "returnCountOnly": "true",
            "f": "json",
        }
        url = f"{VIC_PORTAL_BASE}/{service}/query"
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return False
        data = resp.json()
        return data.get("count", 0) > 0
    except Exception as e:
        logger.debug(f"VIC overlay check failed ({overlay_type}): {e}")
        return False


def _is_in_flight_path(lat: float, lon: float, city: str) -> bool:
    from .station_distance import haversine_m
    airport = FLIGHT_PATH_RADIUS_KM.get(city)
    if not airport:
        return False
    dist = haversine_m(lat, lon, airport["lat"], airport["lon"])
    return dist <= airport["radius_km"] * 1000


def check_overlays(lat: float, lon: float, state: str, city: str) -> Dict[str, bool]:
    result = {
        "flood_zone": False,
        "bushfire_zone": False,
        "heritage_overlay": False,
        "flight_path": False,
    }

    result["flight_path"] = _is_in_flight_path(lat, lon, city)

    if state == "NSW":
        result["flood_zone"] = _check_nsw_overlay(lat, lon, "flood")
        time.sleep(0.3)
        result["bushfire_zone"] = _check_nsw_overlay(lat, lon, "bushfire")
        time.sleep(0.3)
        result["heritage_overlay"] = _check_nsw_overlay(lat, lon, "heritage")
    elif state == "VIC":
        result["flood_zone"] = _check_vic_overlay(lat, lon, "flood")
        time.sleep(0.3)
        result["bushfire_zone"] = _check_vic_overlay(lat, lon, "bushfire")
        time.sleep(0.3)
        result["heritage_overlay"] = _check_vic_overlay(lat, lon, "heritage")

    return result


def enrich_listing_overlays(listing: Listing) -> None:
    if not listing.lat or not listing.lon:
        return

    state = listing.state or ("NSW" if listing.city == "sydney" else "VIC")
    overlays = check_overlays(listing.lat, listing.lon, state, listing.city)

    listing.flood_zone = overlays["flood_zone"]
    listing.bushfire_zone = overlays["bushfire_zone"]
    listing.heritage_overlay = overlays["heritage_overlay"]
    listing.flight_path = overlays["flight_path"]


def run_overlay_enrichment(cfg: Config) -> None:
    logger.info("Running planning overlay enrichment")
    with get_session() as session:
        listings = session.query(Listing).filter(
            Listing.flood_zone.is_(None),
            Listing.is_active == True,
            Listing.lat.isnot(None),
        ).limit(200).all()

        logger.info(f"Checking overlays for {len(listings)} listings")
        for listing in listings:
            try:
                enrich_listing_overlays(listing)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Overlay enrichment error {listing.external_id}: {e}")
