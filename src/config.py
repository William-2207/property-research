from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import yaml


@dataclass
class StationDistanceCfg:
    premium_max_m: int = 400
    good_max_m: int = 800
    acceptable_max_m: int = 1200
    penalty_start_m: int = 800
    penalty_per_200m: float = 1.5
    penalty_beyond_1200m_flat: float = 2.0


@dataclass
class BorderProximityCfg:
    radius_m: int = 500
    threshold_pct: float = 15.0
    discount_factor: float = 0.3
    discount_cap_pct: float = 12.0


@dataclass
class ComparablesCfg:
    primary_radius_m: int = 400
    expanded_radius_m: int = 800
    area_tolerance_pct: float = 20.0
    months_lookback: int = 12


@dataclass
class ApartmentFlagsCfg:
    strata_levy_pct_threshold: float = 1.2
    building_age_years_threshold: int = 20
    single_investor_ownership_pct: float = 20.0
    da_pipeline_unit_threshold: int = 400


@dataclass
class PriceCaps:
    house_suburb_median_max: int = 1_300_000
    unit_suburb_median_max: int = 1_200_000
    listing_max_price: int = 1_200_000


@dataclass
class StrategyWeights:
    growth: float = 0.40
    yield_: float = 0.25
    undervalue: float = 0.25
    risk: float = 0.10


@dataclass
class ScoreSubWeights:
    growth_score: Dict[str, float] = field(default_factory=lambda: {
        "population_growth": 0.20,
        "income_growth": 0.15,
        "infra_spend": 0.20,
        "supply_tightness": 0.15,
        "price_momentum": 0.15,
        "clearance_rate_trend": 0.15,
    })
    yield_score: Dict[str, float] = field(default_factory=lambda: {
        "gross_yield_vs_lga": 0.40,
        "vacancy_rate": 0.35,
        "rent_growth": 0.25,
    })
    risk_score: Dict[str, float] = field(default_factory=lambda: {
        "da_pipeline": 0.25,
        "vacancy_trend": 0.20,
        "overlays": 0.20,
        "investor_concentration": 0.20,
        "heritage_overlay": 0.15,
    })


@dataclass
class CityConfig:
    lga_radius_km: int = 60
    cbd_lat: float = 0.0
    cbd_lon: float = 0.0
    gtfs_feed_url: str = ""


@dataclass
class ApiKeys:
    domain_client_id: str = ""
    domain_client_secret: str = ""
    claude_api_key: str = ""


@dataclass
class Config:
    cities: List[str] = field(default_factory=lambda: ["sydney", "melbourne"])
    price_caps: PriceCaps = field(default_factory=PriceCaps)
    undervalue_min_threshold: float = 5.0
    undervalue_min_comparables: int = 4
    strategy_weights: StrategyWeights = field(default_factory=StrategyWeights)
    score_sub_weights: ScoreSubWeights = field(default_factory=ScoreSubWeights)
    station_distance: StationDistanceCfg = field(default_factory=StationDistanceCfg)
    border_proximity: BorderProximityCfg = field(default_factory=BorderProximityCfg)
    comparables: ComparablesCfg = field(default_factory=ComparablesCfg)
    apartment_flags: ApartmentFlagsCfg = field(default_factory=ApartmentFlagsCfg)
    vendor_motivation_dom_multiplier: float = 1.3
    vendor_motivation_bonus: float = 5.0
    output_dir: Path = Path("./outputs")
    db_path: Path = Path("./data/property_agent.db")
    api_keys: ApiKeys = field(default_factory=ApiKeys)
    sydney: CityConfig = field(default_factory=CityConfig)
    melbourne: CityConfig = field(default_factory=CityConfig)
    log_level: str = "INFO"
    log_file: str = "./data/agent.log"


def load_config(path: str = "config.yaml") -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = Config()
    cfg.cities = raw.get("cities", cfg.cities)
    cfg.undervalue_min_threshold = raw.get("undervalue_min_threshold", 5.0)
    cfg.undervalue_min_comparables = raw.get("undervalue_min_comparables", 4)
    cfg.output_dir = Path(raw.get("output_dir", "./outputs"))
    cfg.db_path = Path(raw.get("db_path", "./data/property_agent.db"))

    if pc := raw.get("price_caps"):
        cfg.price_caps = PriceCaps(
            house_suburb_median_max=pc.get("house_suburb_median_max", 1_300_000),
            unit_suburb_median_max=pc.get("unit_suburb_median_max", 1_200_000),
            listing_max_price=pc.get("listing_max_price", 1_200_000),
        )

    if sw := raw.get("strategy_weights"):
        cfg.strategy_weights = StrategyWeights(
            growth=sw.get("growth", 0.40),
            yield_=sw.get("yield", 0.25),
            undervalue=sw.get("undervalue", 0.25),
            risk=sw.get("risk", 0.10),
        )

    if gw := raw.get("growth_score_weights"):
        cfg.score_sub_weights.growth_score = gw

    if yw := raw.get("yield_score_weights"):
        cfg.score_sub_weights.yield_score = yw

    if rw := raw.get("risk_score_weights"):
        cfg.score_sub_weights.risk_score = rw

    if sd := raw.get("station_distance"):
        cfg.station_distance = StationDistanceCfg(**sd)

    if bp := raw.get("border_proximity"):
        cfg.border_proximity = BorderProximityCfg(**bp)

    if comp := raw.get("comparables"):
        cfg.comparables = ComparablesCfg(**comp)

    if af := raw.get("apartment_flags"):
        cfg.apartment_flags = ApartmentFlagsCfg(**af)

    if vm := raw.get("vendor_motivation"):
        cfg.vendor_motivation_dom_multiplier = vm.get("dom_multiplier_threshold", 1.3)
        cfg.vendor_motivation_bonus = vm.get("bonus_points", 5.0)

    if ak := raw.get("api_keys"):
        cfg.api_keys = ApiKeys(
            domain_client_id=ak.get("domain_client_id", ""),
            domain_client_secret=ak.get("domain_client_secret", ""),
            claude_api_key=ak.get("claude_api_key", ""),
        )

    if sy := raw.get("sydney"):
        cfg.sydney = CityConfig(**sy)

    if me := raw.get("melbourne"):
        cfg.melbourne = CityConfig(**me)

    if lg := raw.get("logging"):
        cfg.log_level = lg.get("level", "INFO")
        cfg.log_file = lg.get("file", "./data/agent.log")

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)

    return cfg
