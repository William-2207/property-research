"""
Comparable sales engine.
For each listing: pull all sales within 400m (expanding to 800m if thin),
same property type, ±20% area, last 12 months.
Computes: median price/sqm from comparables, sample size, confidence flag.
Never falls back to suburb median.
"""
from __future__ import annotations
import statistics
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from loguru import logger
from sqlalchemy import and_

from ..config import Config
from ..database.session import get_session
from ..database.models import ComparableSale, Listing
from .station_distance import haversine_m


def _get_comparables(
    session,
    lat: float,
    lon: float,
    asset_type: str,
    area_sqm: Optional[float],
    radius_m: float,
    months_back: int,
    area_tolerance_pct: float,
) -> List[ComparableSale]:
    cutoff = datetime.utcnow() - timedelta(days=months_back * 30)
    sales = session.query(ComparableSale).filter(
        ComparableSale.asset_type == asset_type,
        ComparableSale.sale_date >= cutoff,
        ComparableSale.price_per_sqm.isnot(None),
        ComparableSale.price_per_sqm > 0,
    ).all()

    nearby = []
    for sale in sales:
        if not sale.lat or not sale.lon:
            continue
        dist = haversine_m(lat, lon, sale.lat, sale.lon)
        if dist > radius_m:
            continue
        if area_sqm and sale.floor_area_sqm:
            area_ratio = abs(sale.floor_area_sqm - area_sqm) / area_sqm
            if area_ratio > area_tolerance_pct / 100:
                continue
        nearby.append(sale)
    return nearby


def compute_comparable_metrics(
    listing: Listing, cfg: Config, session
) -> Tuple[Optional[float], int, int, bool]:
    """
    Returns (fair_value_per_sqm, comparable_count, radius_used_m, thin_flag).
    Never falls back to suburb median.
    """
    if not listing.lat or not listing.lon:
        return None, 0, 0, True

    area = listing.floor_area_sqm or listing.land_size_sqm
    months = cfg.comparables.months_lookback
    tol = cfg.comparables.area_tolerance_pct

    comparables = _get_comparables(
        session,
        listing.lat, listing.lon,
        listing.asset_type,
        area,
        cfg.comparables.primary_radius_m,
        months,
        tol,
    )

    radius_used = cfg.comparables.primary_radius_m
    thin = False

    if len(comparables) < cfg.undervalue_min_comparables:
        comparables = _get_comparables(
            session,
            listing.lat, listing.lon,
            listing.asset_type,
            area,
            cfg.comparables.expanded_radius_m,
            months,
            tol,
        )
        radius_used = cfg.comparables.expanded_radius_m
        thin = True

    if not comparables:
        return None, 0, radius_used, True

    prices_per_sqm = [s.price_per_sqm for s in comparables if s.price_per_sqm]
    if not prices_per_sqm:
        return None, 0, radius_used, True

    median_ppsqm = statistics.median(prices_per_sqm)
    return median_ppsqm, len(comparables), radius_used, thin


def compute_estimated_fair_value(
    listing: Listing, cfg: Config, session
) -> Optional[float]:
    comp_ppsqm, comp_count, radius_m, thin = compute_comparable_metrics(
        listing, cfg, session
    )
    if comp_ppsqm is None:
        return None

    area = listing.floor_area_sqm or listing.land_size_sqm
    if not area or area <= 0:
        return None

    fair_value = comp_ppsqm * area

    listing.comparable_price_per_sqm = round(comp_ppsqm, 2)
    listing.comparable_count = comp_count
    listing.comparable_radius_m = radius_m
    listing.thin_comparables = thin

    return round(fair_value, 0)


def enrich_listing_undervalue(listing: Listing, cfg: Config, session) -> None:
    if not listing.list_price or listing.list_price <= 0:
        return

    fair_value = compute_estimated_fair_value(listing, cfg, session)
    if fair_value is None:
        return

    listing.estimated_fair_value = fair_value
    undervalue_pct = (fair_value - listing.list_price) / fair_value * 100
    listing.undervalue_pct = round(undervalue_pct, 2)

    from .station_distance import station_distance_penalty
    from .border_proximity import compute_border_discount

    undervalue_score = undervalue_pct

    if listing.station_distance_m:
        penalty = station_distance_penalty(listing.station_distance_m, cfg.station_distance)
        undervalue_score -= penalty

    if listing.border_proximity_flag and listing.border_discount_applied_pct:
        undervalue_score -= listing.border_discount_applied_pct

    if listing.thin_comparables:
        undervalue_score *= 0.70

    dom_threshold = (listing.suburb_avg_dom or 45) * cfg.vendor_motivation_dom_multiplier
    if (listing.days_on_market or 0) > dom_threshold and (listing.price_drop_count or 0) >= 1:
        undervalue_score += cfg.vendor_motivation_bonus

    listing.undervalue_score = round(max(0, undervalue_score), 2)


def run_comparables_enrichment(cfg: Config) -> None:
    logger.info("Running comparables enrichment")
    with get_session() as session:
        listings = session.query(Listing).filter(
            Listing.estimated_fair_value.is_(None),
            Listing.is_active == True,
            Listing.list_price <= cfg.price_caps.listing_max_price,
        ).limit(500).all()

        logger.info(f"Computing comparables for {len(listings)} listings")
        enriched = 0
        for listing in listings:
            try:
                enrich_listing_undervalue(listing, cfg, session)
                enriched += 1
            except Exception as e:
                logger.error(f"Comparables error {listing.external_id}: {e}")

        logger.info(f"Enriched {enriched} listings with comparables data")
