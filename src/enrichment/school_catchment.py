"""
School catchment ICSEA enrichment.
Looks up the catchment zone for a specific listing address and returns
the ICSEA score of the nearest primary and secondary school.
Data sourced from My School (ACARA) and state education portals.
"""
from __future__ import annotations
import time
from typing import Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session
from ..database.models import Listing

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}

MYSCHOOL_SEARCH_URL = "https://www.myschool.edu.au/api/school/search"

NSW_CATCHMENT_BASE = "https://education.nsw.gov.au/school-finder"
VIC_CATCHMENT_BASE = "https://www.findmyschool.vic.gov.au/api/v1/schools"


def _search_myschool_nearby(lat: float, lon: float, school_type: str) -> Optional[Dict]:
    try:
        params = {
            "lat": lat,
            "lng": lon,
            "schoolType": school_type,
            "radius": 3,
            "pageSize": 5,
        }
        resp = requests.get(MYSCHOOL_SEARCH_URL, params=params, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        schools = data.get("schools", data.get("results", []))
        if not schools:
            return None

        for school in schools:
            icsea = school.get("icsea") or school.get("icsea_value") or school.get("ICSEAValue")
            if icsea and int(icsea) > 0:
                return {
                    "name": school.get("name", school.get("schoolName", "")),
                    "icsea": int(icsea),
                    "type": school_type,
                }
    except Exception as e:
        logger.debug(f"MySchool search failed: {e}")
    return None


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def _get_nsw_catchment_school(lat: float, lon: float, school_type: str) -> Optional[Dict]:
    try:
        params = {
            "lat": lat,
            "lng": lon,
            "schooltype": "primary" if school_type == "primary" else "high",
        }
        resp = requests.get(
            "https://education.nsw.gov.au/school-finder/api/catchment",
            params=params, headers=HEADERS, timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, dict):
                school_name = data.get("schoolName", data.get("name", ""))
                icsea = data.get("icsea") or data.get("icseaValue")
                if school_name:
                    return {"name": school_name, "icsea": int(icsea) if icsea else None}
    except Exception as e:
        logger.debug(f"NSW catchment lookup failed: {e}")
    return None


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def _get_vic_catchment_school(lat: float, lon: float, school_type: str) -> Optional[Dict]:
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "type": "primary" if school_type == "primary" else "secondary",
        }
        resp = requests.get(VIC_CATCHMENT_BASE, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            schools = data.get("schools", data if isinstance(data, list) else [])
            if schools:
                school = schools[0]
                return {
                    "name": school.get("name", school.get("schoolName", "")),
                    "icsea": school.get("icsea") or school.get("icseaValue"),
                }
    except Exception as e:
        logger.debug(f"VIC catchment lookup failed: {e}")
    return None


def lookup_school_catchment(lat: float, lon: float, state: str) -> Dict:
    result = {
        "primary_school": None,
        "primary_school_icsea": None,
        "secondary_school": None,
        "secondary_school_icsea": None,
    }

    if state == "NSW":
        primary = _get_nsw_catchment_school(lat, lon, "primary")
        time.sleep(0.5)
        secondary = _get_nsw_catchment_school(lat, lon, "secondary")
    elif state == "VIC":
        primary = _get_vic_catchment_school(lat, lon, "primary")
        time.sleep(0.5)
        secondary = _get_vic_catchment_school(lat, lon, "secondary")
    else:
        primary = _search_myschool_nearby(lat, lon, "primary")
        time.sleep(0.5)
        secondary = _search_myschool_nearby(lat, lon, "secondary")

    if not primary:
        primary = _search_myschool_nearby(lat, lon, "primary")
    if not secondary:
        secondary = _search_myschool_nearby(lat, lon, "secondary")

    if primary:
        result["primary_school"] = primary.get("name")
        result["primary_school_icsea"] = primary.get("icsea")

    if secondary:
        result["secondary_school"] = secondary.get("name")
        result["secondary_school_icsea"] = secondary.get("icsea")

    return result


def enrich_listing_schools(listing: Listing) -> None:
    if not listing.lat or not listing.lon:
        return

    state = listing.state or ("NSW" if listing.city == "sydney" else "VIC")
    catchment = lookup_school_catchment(listing.lat, listing.lon, state)

    listing.primary_school = catchment.get("primary_school")
    listing.primary_school_icsea = catchment.get("primary_school_icsea")
    listing.secondary_school = catchment.get("secondary_school")
    listing.secondary_school_icsea = catchment.get("secondary_school_icsea")


def run_school_enrichment(cfg: Config) -> None:
    logger.info("Running school catchment enrichment")
    with get_session() as session:
        listings = session.query(Listing).filter(
            Listing.primary_school.is_(None),
            Listing.is_active == True,
            Listing.lat.isnot(None),
        ).limit(200).all()

        logger.info(f"Enriching school data for {len(listings)} listings")
        for listing in listings:
            try:
                enrich_listing_schools(listing)
                time.sleep(1.0)
            except Exception as e:
                logger.error(f"School enrichment error {listing.external_id}: {e}")
