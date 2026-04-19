"""
Border proximity enrichment.
For each listing, identifies neighbouring suburbs within 500m and checks
whether any neighbour has a median price >15% lower than the listing suburb.
Computes discount factor per spec: (gap% - 15%) × 0.3, capped at 12%.
"""
from __future__ import annotations
import json
from typing import Dict, List, Optional, Tuple
import requests
from loguru import logger

from ..config import Config
from ..database.session import get_session
from ..database.models import Listing, Suburb, SuburbFundamentals
from .station_distance import haversine_m

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _get_suburbs_within_radius(lat: float, lon: float, radius_m: float) -> List[Dict]:
    delta_deg = radius_m / 111000
    bbox = (lat - delta_deg, lon - delta_deg, lat + delta_deg, lon + delta_deg)
    query = f"""
[out:json][timeout:30];
(
  way["place"="suburb"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
  relation["place"="suburb"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
);
out center tags;
"""
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=45)
        resp.raise_for_status()
        data = resp.json()
        suburbs = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name", "")
            centre = el.get("center", {})
            if name and centre:
                suburbs.append({
                    "name": name,
                    "lat": centre.get("lat"),
                    "lon": centre.get("lon"),
                })
        return suburbs
    except Exception as e:
        logger.debug(f"Overpass border lookup failed: {e}")
        return []


def _get_suburb_median(suburb_name: str, state: str, asset_type: str, session) -> Optional[float]:
    suburb = session.query(Suburb).filter_by(name=suburb_name, state=state).first()
    if not suburb:
        name_lower = suburb_name.lower()
        suburb = session.query(Suburb).filter(
            Suburb.name.ilike(f"%{name_lower}%"),
            Suburb.state == state,
        ).first()
    if not suburb:
        return None

    fundamentals = session.query(SuburbFundamentals).filter_by(
        suburb_id=suburb.id,
        asset_type=asset_type,
    ).order_by(SuburbFundamentals.snapshot_month.desc()).first()

    return fundamentals.median_price if fundamentals else None


def compute_border_discount(median_gap_pct: float, cfg: Config) -> float:
    threshold = cfg.border_proximity.threshold_pct
    if median_gap_pct <= threshold:
        return 0.0
    discount = (median_gap_pct - threshold) * cfg.border_proximity.discount_factor
    return round(min(discount, cfg.border_proximity.discount_cap_pct), 2)


def enrich_listing_border_proximity(listing: Listing, cfg: Config, session) -> None:
    if not listing.lat or not listing.lon:
        return

    suburb_median = _get_suburb_median(
        listing.suburb_name, listing.state, listing.asset_type, session
    )
    if not suburb_median or suburb_median <= 0:
        return

    nearby_suburbs = _get_suburbs_within_radius(
        listing.lat, listing.lon, cfg.border_proximity.radius_m
    )

    worst_gap = 0.0
    worst_suburb_name = None
    worst_suburb_distance = None

    for nearby in nearby_suburbs:
        name = nearby.get("name", "")
        if name.lower() == listing.suburb_name.lower():
            continue

        nearby_lat = nearby.get("lat")
        nearby_lon = nearby.get("lon")
        if not nearby_lat or not nearby_lon:
            continue

        distance = haversine_m(listing.lat, listing.lon, nearby_lat, nearby_lon)
        if distance > cfg.border_proximity.radius_m:
            continue

        nearby_median = _get_suburb_median(name, listing.state, listing.asset_type, session)
        if not nearby_median or nearby_median <= 0:
            continue

        gap_pct = (suburb_median - nearby_median) / suburb_median * 100
        if gap_pct > cfg.border_proximity.threshold_pct and gap_pct > worst_gap:
            worst_gap = gap_pct
            worst_suburb_name = name
            worst_suburb_distance = distance

    if worst_suburb_name:
        discount = compute_border_discount(worst_gap, cfg)
        listing.border_proximity_flag = True
        listing.nearest_lower_suburb = worst_suburb_name
        listing.nearest_lower_suburb_distance_m = round(worst_suburb_distance, 1)
        listing.border_median_gap_pct = round(worst_gap, 2)
        listing.border_discount_applied_pct = discount
        logger.debug(
            f"Border flag: {listing.suburb_name} → {worst_suburb_name} "
            f"gap={worst_gap:.1f}% discount={discount:.1f}%"
        )
    else:
        listing.border_proximity_flag = False
        listing.nearest_lower_suburb = None
        listing.border_median_gap_pct = 0.0
        listing.border_discount_applied_pct = 0.0


def run_border_proximity_enrichment(cfg: Config) -> None:
    logger.info("Running border proximity enrichment")
    with get_session() as session:
        listings = session.query(Listing).filter(
            Listing.border_proximity_flag.is_(None),
            Listing.is_active == True,
            Listing.lat.isnot(None),
        ).limit(300).all()

        logger.info(f"Checking border proximity for {len(listings)} listings")
        for listing in listings:
            try:
                enrich_listing_border_proximity(listing, cfg, session)
            except Exception as e:
                logger.error(f"Border enrichment error {listing.external_id}: {e}")
