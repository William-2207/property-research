"""
Scoring engine — computes all 4 dimensions and composite score per suburb + asset type.
Capital growth: 40%, Yield: 25%, Undervalue: 25%, Risk (inverted): 10%.
"""
from __future__ import annotations
import math
from datetime import datetime
from typing import Optional, Tuple
from loguru import logger

from ..config import Config
from ..database.session import get_session
from ..database.models import (
    Suburb, SuburbFundamentals, SuburbScore, Listing, InfrastructureProject
)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _normalise_to_100(value: float, min_val: float, max_val: float,
                      invert: bool = False) -> float:
    if max_val == min_val:
        return 50.0
    score = (value - min_val) / (max_val - min_val) * 100
    return _clamp(100 - score if invert else score)


def score_growth(f: SuburbFundamentals, cfg: Config,
                 infra_spend: float) -> float:
    weights = cfg.score_sub_weights.growth_score
    score = 0.0

    pop_growth = f.population_growth_rate or 0.0
    pop_score = _normalise_to_100(pop_growth, -1.0, 3.0)
    score += pop_score * weights.get("population_growth", 0.20)

    income_growth = f.income_growth_rate or 0.0
    income_score = _normalise_to_100(income_growth, 0.0, 5.0)
    score += income_score * weights.get("income_growth", 0.15)

    infra_score = _normalise_to_100(infra_spend, 0, 5e9)
    score += infra_score * weights.get("infra_spend", 0.20)

    tightness = f.supply_tightness_ratio or 1.0
    tightness_score = _normalise_to_100(tightness, 0.5, 2.0, invert=True)
    score += tightness_score * weights.get("supply_tightness", 0.15)

    momentum = f.price_momentum_pct or 0.0
    momentum_score = _normalise_to_100(momentum, -10.0, 20.0)
    score += momentum_score * weights.get("price_momentum", 0.15)

    clearance = f.auction_clearance_12m_avg or 50.0
    clearance_score = _normalise_to_100(clearance, 40.0, 90.0)
    score += clearance_score * weights.get("clearance_rate_trend", 0.15)

    return _clamp(score)


def score_yield(f: SuburbFundamentals, cfg: Config) -> float:
    weights = cfg.score_sub_weights.yield_score
    score = 0.0

    gross_yield = f.gross_yield_pct or 0.0
    lga_avg = f.lga_avg_yield_pct or 3.5
    yield_delta = gross_yield - lga_avg
    yield_score = _normalise_to_100(yield_delta, -2.0, 2.0)
    score += yield_score * weights.get("gross_yield_vs_lga", 0.40)

    vacancy = f.vacancy_rate or 2.0
    vacancy_score = _normalise_to_100(vacancy, 0.0, 5.0, invert=True)
    score += vacancy_score * weights.get("vacancy_rate", 0.35)

    rent_growth = f.rent_growth_12m_pct or 0.0
    rent_score = _normalise_to_100(rent_growth, -5.0, 15.0)
    score += rent_score * weights.get("rent_growth", 0.25)

    return _clamp(score)


def score_suburb_undervalue(suburb_id: int, asset_type: str, session) -> float:
    listings = session.query(Listing).filter(
        Listing.suburb_id == suburb_id,
        Listing.asset_type == asset_type,
        Listing.undervalue_score.isnot(None),
        Listing.undervalue_score > 0,
        Listing.is_active == True,
    ).all()

    if not listings:
        return 50.0

    scores = [l.undervalue_score for l in listings if l.undervalue_score]
    if not scores:
        return 50.0

    avg_undervalue = sum(scores) / len(scores)
    return _clamp(_normalise_to_100(avg_undervalue, 0, 30))


def score_risk(f: SuburbFundamentals, cfg: Config,
               heritage_listing_flag: bool = False) -> float:
    weights = cfg.score_sub_weights.risk_score
    risk_raw = 0.0

    da_ratio = f.da_to_stock_ratio or 0.0
    da_score = _normalise_to_100(da_ratio, 0.0, 0.5)
    risk_raw += da_score * weights.get("da_pipeline", 0.25)

    vacancy_trend = f.vacancy_trend or "stable"
    trend_score = {"falling": 0, "stable": 50, "rising": 100}.get(vacancy_trend, 50)
    risk_raw += trend_score * weights.get("vacancy_trend", 0.20)

    overlay_count = sum([
        bool(f.flood_zone), bool(f.bushfire_zone), bool(f.flight_path)
    ])
    overlay_score = _clamp(overlay_count / 3 * 100)
    risk_raw += overlay_score * weights.get("overlays", 0.20)

    investor_ratio = f.investor_ratio_pct or 30.0
    investor_score = 100.0 if investor_ratio > 50 else _normalise_to_100(investor_ratio, 20, 50)
    risk_raw += investor_score * weights.get("investor_concentration", 0.20)

    heritage_score = 100.0 if heritage_listing_flag else 0.0
    risk_raw += heritage_score * weights.get("heritage_overlay", 0.15)

    return _clamp(100.0 - risk_raw)


def compute_composite(growth: float, yield_s: float, undervalue: float,
                      risk: float, cfg: Config) -> float:
    w = cfg.strategy_weights
    return _clamp(
        growth * w.growth +
        yield_s * w.yield_ +
        undervalue * w.undervalue +
        risk * w.risk
    )


def _get_infra_spend(suburb_id: int, city: str, session) -> float:
    from datetime import timedelta
    cutoff_5yr = datetime.utcnow().replace(year=datetime.utcnow().year + 5)
    projects = session.query(InfrastructureProject).filter(
        InfrastructureProject.suburb_id == suburb_id,
        InfrastructureProject.estimated_value.isnot(None),
    ).all()
    total = sum(p.estimated_value for p in projects if p.estimated_value)
    return total


def score_suburb(suburb: Suburb, asset_type: str, snapshot_month: str,
                 cfg: Config, session) -> Optional[SuburbScore]:
    fundamentals = session.query(SuburbFundamentals).filter_by(
        suburb_id=suburb.id,
        snapshot_month=snapshot_month,
        asset_type=asset_type,
    ).first()

    if not fundamentals:
        logger.debug(f"No fundamentals for {suburb.name} {asset_type} {snapshot_month}")
        return None

    price_cap = (cfg.price_caps.house_suburb_median_max if asset_type == "house"
                 else cfg.price_caps.unit_suburb_median_max)
    if fundamentals.median_price and fundamentals.median_price > price_cap:
        return None

    infra_spend = _get_infra_spend(suburb.id, suburb.city, session)

    growth = score_growth(fundamentals, cfg, infra_spend)
    yield_s = score_yield(fundamentals, cfg)
    undervalue = score_suburb_undervalue(suburb.id, asset_type, session)
    risk = score_risk(fundamentals, cfg)
    composite = compute_composite(growth, yield_s, undervalue, risk, cfg)

    prior = session.query(SuburbScore).filter_by(
        suburb_id=suburb.id,
        asset_type=asset_type,
    ).order_by(SuburbScore.snapshot_month.desc()).first()

    prior_composite = prior.composite_score if prior else None
    change_pct = None
    if prior_composite and prior_composite > 0:
        change_pct = (composite - prior_composite) / prior_composite * 100

    existing = session.query(SuburbScore).filter_by(
        suburb_id=suburb.id,
        snapshot_month=snapshot_month,
        asset_type=asset_type,
    ).first()

    if existing:
        existing.growth_score = growth
        existing.yield_score = yield_s
        existing.undervalue_score = undervalue
        existing.risk_score = risk
        existing.composite_score = composite
        existing.composite_score_prior = prior_composite
        existing.composite_change_pct = change_pct
        return existing

    score_row = SuburbScore(
        suburb_id=suburb.id,
        snapshot_month=snapshot_month,
        asset_type=asset_type,
        city=suburb.city,
        growth_score=round(growth, 2),
        yield_score=round(yield_s, 2),
        undervalue_score=round(undervalue, 2),
        risk_score=round(risk, 2),
        composite_score=round(composite, 2),
        composite_score_prior=prior_composite,
        composite_change_pct=round(change_pct, 2) if change_pct else None,
    )
    session.add(score_row)
    return score_row


def score_listing(listing: Listing, suburb_score: Optional[SuburbScore],
                  cfg: Config) -> None:
    if suburb_score:
        listing.composite_score = suburb_score.composite_score


def run_scoring_engine(cfg: Config) -> None:
    snapshot_month = datetime.utcnow().strftime("%Y-%m")
    logger.info(f"Running scoring engine for {snapshot_month}")

    with get_session() as session:
        suburbs = session.query(Suburb).all()
        scored_count = 0

        for suburb in suburbs:
            if suburb.city not in cfg.cities:
                continue
            for asset_type in ["house", "unit"]:
                try:
                    score_row = score_suburb(suburb, asset_type, snapshot_month, cfg, session)
                    if score_row:
                        scored_count += 1
                except Exception as e:
                    logger.error(f"Scoring error {suburb.name} {asset_type}: {e}")

        session.flush()

        listings = session.query(Listing).filter(
            Listing.is_active == True,
            Listing.composite_score.is_(None),
        ).all()

        for listing in listings:
            suburb_score = session.query(SuburbScore).filter_by(
                suburb_id=listing.suburb_id,
                snapshot_month=snapshot_month,
                asset_type=listing.asset_type,
            ).first()
            score_listing(listing, suburb_score, cfg)

    logger.info(f"Scoring complete — {scored_count} suburb×asset_type combinations scored")
