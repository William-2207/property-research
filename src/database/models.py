from __future__ import annotations
from datetime import datetime
from sqlalchemy import (
    Column, Integer, Float, String, Boolean, DateTime, Text,
    ForeignKey, UniqueConstraint, Index, create_engine
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Suburb(Base):
    __tablename__ = "suburbs"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    state = Column(String(10), nullable=False)
    city = Column(String(20), nullable=False)
    lga = Column(String(100))
    postcode = Column(String(10))
    lat = Column(Float)
    lon = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("name", "state", name="uq_suburb_name_state"),
    )

    fundamentals = relationship("SuburbFundamentals", back_populates="suburb", cascade="all, delete-orphan")
    listings = relationship("Listing", back_populates="suburb")
    infra_projects = relationship("InfrastructureProject", back_populates="suburb")
    council_alerts = relationship("CouncilAlert", back_populates="suburb")
    scores = relationship("SuburbScore", back_populates="suburb", cascade="all, delete-orphan")


class SuburbFundamentals(Base):
    __tablename__ = "suburb_fundamentals"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"), nullable=False)
    snapshot_month = Column(String(7), nullable=False)  # YYYY-MM
    asset_type = Column(String(20), nullable=False)  # house / unit

    median_price = Column(Float)
    median_price_12m_ago = Column(Float)
    price_momentum_pct = Column(Float)

    auction_clearance_rate = Column(Float)
    auction_clearance_12m_avg = Column(Float)
    stock_on_market = Column(Integer)
    stock_12m_avg = Column(Float)
    supply_tightness_ratio = Column(Float)

    vacancy_rate = Column(Float)
    vacancy_rate_prior = Column(Float)
    vacancy_trend = Column(String(10))  # rising / falling / stable
    median_weekly_rent = Column(Float)
    rent_growth_12m_pct = Column(Float)
    gross_yield_pct = Column(Float)
    lga_avg_yield_pct = Column(Float)

    population_growth_rate = Column(Float)
    net_internal_migration = Column(Float)
    median_household_income = Column(Float)
    income_growth_rate = Column(Float)
    owner_occupier_ratio_pct = Column(Float)
    investor_ratio_pct = Column(Float)

    da_approved_count = Column(Integer)
    da_lodged_count = Column(Integer)
    da_pipeline_units = Column(Integer)
    da_to_stock_ratio = Column(Float)

    infra_spend_5km_5yr = Column(Float)
    top_school_icsea = Column(Integer)
    nearest_primary_school = Column(String(200))
    nearest_primary_icsea = Column(Integer)
    nearest_secondary_school = Column(String(200))
    nearest_secondary_icsea = Column(Integer)

    avg_station_distance_m = Column(Float)
    walk_score = Column(Integer)
    pt_access_score = Column(Integer)

    flood_zone = Column(Boolean, default=False)
    bushfire_zone = Column(Boolean, default=False)
    flight_path = Column(Boolean, default=False)
    heritage_conservation = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("suburb_id", "snapshot_month", "asset_type", name="uq_fundamentals_month_type"),
        Index("ix_fundamentals_suburb_month", "suburb_id", "snapshot_month"),
    )

    suburb = relationship("Suburb", back_populates="fundamentals")


class Listing(Base):
    __tablename__ = "listings"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"))
    city = Column(String(20), nullable=False)

    external_id = Column(String(100), unique=True, nullable=False)
    source = Column(String(20), nullable=False)  # domain / rea
    url = Column(String(500))

    address = Column(String(300), nullable=False)
    suburb_name = Column(String(100))
    state = Column(String(10))
    postcode = Column(String(10))
    lat = Column(Float)
    lon = Column(Float)

    property_type = Column(String(30), nullable=False)  # house / apartment / townhouse
    asset_type = Column(String(20), nullable=False)  # house / unit
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    car_spaces = Column(Integer)
    floor_area_sqm = Column(Float)
    land_size_sqm = Column(Float)

    list_price = Column(Float)
    price_per_sqm = Column(Float)
    sale_method = Column(String(50))  # auction / private treaty / EOI
    listing_agent = Column(String(200))

    days_on_market = Column(Integer)
    suburb_avg_dom = Column(Float)
    dom_vs_avg_ratio = Column(Float)
    price_drop_count = Column(Integer, default=0)
    vendor_motivation_score = Column(Float, default=0.0)

    date_first_listed = Column(DateTime)
    date_last_updated = Column(DateTime)
    is_active = Column(Boolean, default=True)

    estimated_fair_value = Column(Float)
    undervalue_pct = Column(Float)
    undervalue_score = Column(Float)
    comparable_price_per_sqm = Column(Float)
    comparable_count = Column(Integer)
    comparable_radius_m = Column(Integer)
    thin_comparables = Column(Boolean, default=False)

    station_distance_m = Column(Float)
    station_name = Column(String(200))
    station_category = Column(String(20))

    border_proximity_flag = Column(Boolean, default=False)
    nearest_lower_suburb = Column(String(100))
    nearest_lower_suburb_distance_m = Column(Float)
    border_median_gap_pct = Column(Float)
    border_discount_applied_pct = Column(Float)

    flood_zone = Column(Boolean, default=False)
    bushfire_zone = Column(Boolean, default=False)
    flight_path = Column(Boolean, default=False)
    heritage_overlay = Column(Boolean, default=False)

    primary_school = Column(String(200))
    primary_school_icsea = Column(Integer)
    secondary_school = Column(String(200))
    secondary_school_icsea = Column(Integer)

    strata_levy_est_pct = Column(Float)
    building_age_years = Column(Integer)
    cladding_risk = Column(Boolean, default=False)
    high_investor_concentration = Column(Boolean, default=False)
    high_da_pipeline = Column(Boolean, default=False)

    composite_score = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    suburb = relationship("Suburb", back_populates="listings")
    price_history = relationship("ListingPriceHistory", back_populates="listing", cascade="all, delete-orphan")


class ListingPriceHistory(Base):
    __tablename__ = "listing_price_history"

    id = Column(Integer, primary_key=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), nullable=False)
    price = Column(Float, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow)
    source = Column(String(20))

    listing = relationship("Listing", back_populates="price_history")


class ComparableSale(Base):
    __tablename__ = "comparable_sales"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"))
    city = Column(String(20))
    address = Column(String(300))
    lat = Column(Float)
    lon = Column(Float)
    asset_type = Column(String(20))
    property_type = Column(String(30))
    sale_price = Column(Float)
    floor_area_sqm = Column(Float)
    land_size_sqm = Column(Float)
    price_per_sqm = Column(Float)
    sale_date = Column(DateTime)
    source = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_comparable_sales_location", "lat", "lon"),
        Index("ix_comparable_sales_suburb_type", "suburb_id", "asset_type"),
    )


class InfrastructureProject(Base):
    __tablename__ = "infrastructure_projects"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"))
    city = Column(String(20))
    project_name = Column(String(500), nullable=False)
    project_type = Column(String(100))
    lga = Column(String(100))
    suburb_name = Column(String(100))
    distance_to_suburb_centre_m = Column(Float)
    estimated_value = Column(Float)
    status = Column(String(100))
    expected_completion_date = Column(DateTime)
    source_url = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    suburb = relationship("Suburb", back_populates="infra_projects")


class CouncilAlert(Base):
    __tablename__ = "council_alerts"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"))
    city = Column(String(20))
    lga = Column(String(100), nullable=False)
    meeting_date = Column(DateTime)
    agenda_item = Column(Text)
    keywords_matched = Column(Text)  # JSON list
    significance = Column(String(10))  # HIGH / MEDIUM / LOW
    source_url = Column(String(500))
    raw_text = Column(Text)
    claude_summary = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    suburb = relationship("Suburb", back_populates="council_alerts")


class SuburbScore(Base):
    __tablename__ = "suburb_scores"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"), nullable=False)
    snapshot_month = Column(String(7), nullable=False)
    asset_type = Column(String(20), nullable=False)
    city = Column(String(20))

    growth_score = Column(Float)
    yield_score = Column(Float)
    undervalue_score = Column(Float)
    risk_score = Column(Float)
    composite_score = Column(Float)
    composite_score_prior = Column(Float)
    composite_change_pct = Column(Float)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("suburb_id", "snapshot_month", "asset_type", name="uq_score_month_type"),
    )

    suburb = relationship("Suburb", back_populates="scores")


class TrainStation(Base):
    __tablename__ = "train_stations"

    id = Column(Integer, primary_key=True)
    city = Column(String(20), nullable=False)
    station_name = Column(String(200), nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    line = Column(String(200))
    station_type = Column(String(50))  # train / metro / tram
    source = Column(String(20))  # gtfs / osm
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_stations_city_location", "city", "lat", "lon"),
    )


class SuburbBoundary(Base):
    __tablename__ = "suburb_boundaries"

    id = Column(Integer, primary_key=True)
    suburb_id = Column(Integer, ForeignKey("suburbs.id"), unique=True)
    geojson = Column(Text)
    centre_lat = Column(Float)
    centre_lon = Column(Float)
    area_sqkm = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db(db_path: str) -> None:
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return engine
