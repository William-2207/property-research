"""
Scheduler — runs weekly listings scrapes and monthly fundamentals scrapes.
Uses the 'schedule' library to run as a long-running process.
"""
from __future__ import annotations
import sys
import time
from datetime import datetime
import schedule
from loguru import logger

from .config import Config, load_config
from .database.session import init_engine
from .scrapers.listings import run_listings_scrape
from .scrapers.fundamentals import run_fundamentals_scrape
from .scrapers.infrastructure import run_infrastructure_scrape
from .scrapers.council import run_council_scrape
from .enrichment.station_distance import run_station_enrichment
from .enrichment.border_proximity import run_border_proximity_enrichment
from .enrichment.comparables import run_comparables_enrichment
from .enrichment.overlays import run_overlay_enrichment
from .enrichment.school_catchment import run_school_enrichment
from .scoring.engine import run_scoring_engine
from .outputs.excel_generator import generate_excel_workbook
from .outputs.dashboard_generator import generate_html_dashboard


def run_weekly_pipeline(cfg: Config) -> None:
    logger.info("=== WEEKLY PIPELINE START ===")
    try:
        run_listings_scrape(cfg)
        run_station_enrichment(cfg)
        run_border_proximity_enrichment(cfg)
        run_comparables_enrichment(cfg)
        run_overlay_enrichment(cfg)
        run_school_enrichment(cfg)
        run_scoring_engine(cfg)
        generate_excel_workbook(cfg)
        generate_html_dashboard(cfg)
        logger.info("=== WEEKLY PIPELINE COMPLETE ===")
    except Exception as e:
        logger.error(f"Weekly pipeline failed: {e}")
        raise


def run_monthly_pipeline(cfg: Config) -> None:
    logger.info("=== MONTHLY PIPELINE START ===")
    try:
        run_fundamentals_scrape(cfg)
        run_infrastructure_scrape(cfg)
        run_council_scrape(cfg)
        run_scoring_engine(cfg)
        generate_excel_workbook(cfg)
        generate_html_dashboard(cfg)
        logger.info("=== MONTHLY PIPELINE COMPLETE ===")
    except Exception as e:
        logger.error(f"Monthly pipeline failed: {e}")
        raise


def run_full_pipeline(cfg: Config) -> None:
    logger.info("=== FULL PIPELINE START ===")
    run_monthly_pipeline(cfg)
    run_weekly_pipeline(cfg)
    logger.info("=== FULL PIPELINE COMPLETE ===")


def start_scheduler(cfg: Config) -> None:
    logger.info("Starting scheduler — weekly listings, monthly fundamentals")

    schedule.every().monday.at("06:00").do(run_weekly_pipeline, cfg=cfg)
    schedule.every().day.at("07:00").do(
        lambda: run_monthly_pipeline(cfg) if datetime.now().day == 1 else None
    )

    logger.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)
