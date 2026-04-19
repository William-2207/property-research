#!/usr/bin/env python3
"""
Property Investment Research Agent
Sydney & Melbourne — capital growth focused suburb and listing analysis.

Usage:
  python main.py --run full          # Full pipeline (fundamentals + listings + outputs)
  python main.py --run weekly        # Listings scrape + enrichment + outputs only
  python main.py --run monthly       # Fundamentals + infra + council + outputs only
  python main.py --run score         # Scoring engine only
  python main.py --run excel         # Regenerate Excel workbook only
  python main.py --run dashboard     # Regenerate HTML dashboard only
  python main.py --run scheduler     # Start long-running scheduler
  python main.py --run test          # Test run: 5 suburbs, Sydney only
  python main.py --config path.yaml  # Use alternate config file
"""
import argparse
import sys
from pathlib import Path
from loguru import logger


def setup_logging(cfg) -> None:
    logger.remove()
    logger.add(sys.stderr, level=cfg.log_level,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add(cfg.log_file, rotation="10 MB", retention="30 days",
               level="DEBUG",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Property Investment Research Agent")
    parser.add_argument("--run", default="full",
                        choices=["full", "weekly", "monthly", "score",
                                 "excel", "dashboard", "scheduler", "test", "stations"],
                        help="Pipeline to run")
    parser.add_argument("--config", default="config.yaml",
                        help="Path to config YAML file")
    parser.add_argument("--city", choices=["sydney", "melbourne"],
                        help="Override: single city only")
    args = parser.parse_args()

    from src.config import load_config
    cfg = load_config(args.config)
    if args.city:
        cfg.cities = [args.city]

    setup_logging(cfg)

    from src.database.session import init_engine
    init_engine(str(cfg.db_path))

    logger.info(f"Property Agent starting — mode: {args.run} | cities: {cfg.cities}")

    if args.run == "test":
        _run_test(cfg)
    elif args.run == "full":
        from src.scheduler import run_full_pipeline
        run_full_pipeline(cfg)
    elif args.run == "weekly":
        from src.scheduler import run_weekly_pipeline
        run_weekly_pipeline(cfg)
    elif args.run == "monthly":
        from src.scheduler import run_monthly_pipeline
        run_monthly_pipeline(cfg)
    elif args.run == "score":
        from src.scoring.engine import run_scoring_engine
        run_scoring_engine(cfg)
    elif args.run == "excel":
        from src.outputs.excel_generator import generate_excel_workbook
        path = generate_excel_workbook(cfg)
        logger.info(f"Excel saved: {path}")
    elif args.run == "dashboard":
        from src.outputs.dashboard_generator import generate_html_dashboard
        path = generate_html_dashboard(cfg)
        logger.info(f"Dashboard saved: {path}")
    elif args.run == "stations":
        from src.enrichment.station_distance import load_stations_for_city
        for city in cfg.cities:
            load_stations_for_city(cfg, city)
    elif args.run == "scheduler":
        from src.scheduler import start_scheduler
        start_scheduler(cfg)
    else:
        logger.error(f"Unknown run mode: {args.run}")
        sys.exit(1)


def _run_test(cfg) -> None:
    """Step 9 test run: 5 Sydney suburbs, verify end-to-end pipeline."""
    logger.info("=== TEST RUN: 5 Sydney suburbs (Playwright scraping) ===")

    original_cities = cfg.cities
    cfg.cities = ["sydney"]

    TEST_SUBURBS = [
        ("Parramatta", "NSW"),
        ("Strathfield", "NSW"),
        ("Burwood", "NSW"),
        ("Homebush", "NSW"),
        ("Auburn", "NSW"),
    ]

    from src.scrapers.fundamentals import _process_suburb
    from src.scrapers.listings import (
        scrape_domain_suburb, scrape_rea_suburb, _upsert_listing, _get_suburb_avg_dom
    )
    from src.scrapers.playwright_base import PlaywrightSession, random_delay
    from src.database.session import get_session, upsert_suburb as _upsert_suburb
    from datetime import datetime

    snapshot_month = datetime.utcnow().strftime("%Y-%m")

    logger.info("Step 2: Suburb fundamentals scrape (5 suburbs via Playwright)")
    with PlaywrightSession() as pw:
        for suburb_name, state in TEST_SUBURBS:
            try:
                _process_suburb(cfg, suburb_name, state, "sydney", snapshot_month, pw)
                logger.info(f"  ✓ Fundamentals: {suburb_name}")
            except Exception as e:
                logger.error(f"  ✗ Fundamentals error {suburb_name}: {e}")
            random_delay(3, 7)

    logger.info("Step 3: Listings scrape (5 suburbs via Playwright)")
    with PlaywrightSession() as pw:
        for suburb_name, state in TEST_SUBURBS:
            all_raw = []
            for asset_type in ["house", "unit"]:
                try:
                    got = scrape_domain_suburb(
                        pw, suburb_name, state, "sydney",
                        cfg.price_caps.listing_max_price, asset_type
                    )
                    all_raw.extend(got)
                    logger.info(f"  Domain {suburb_name} {asset_type}: {len(got)}")
                except Exception as e:
                    logger.error(f"  ✗ Domain error {suburb_name} {asset_type}: {e}")
                random_delay(3, 7)

                try:
                    got_rea = scrape_rea_suburb(
                        pw, suburb_name, state, "sydney",
                        cfg.price_caps.listing_max_price, asset_type
                    )
                    seen = {r["external_id"] for r in all_raw}
                    new_rea = [r for r in got_rea if r["external_id"] not in seen]
                    all_raw.extend(new_rea)
                    logger.info(f"  REA {suburb_name} {asset_type}: {len(got_rea)} (+{len(new_rea)})")
                except Exception as e:
                    logger.error(f"  ✗ REA error {suburb_name} {asset_type}: {e}")
                random_delay(3, 7)

            with get_session() as session:
                suburb_obj = _upsert_suburb(session, suburb_name, state, "sydney")
                count = 0
                for raw in all_raw:
                    if raw.get("list_price", 0) <= cfg.price_caps.listing_max_price:
                        avg_dom = _get_suburb_avg_dom(
                            session, suburb_obj.id, raw.get("asset_type", "house")
                        )
                        _upsert_listing(session, raw, suburb_obj.id, avg_dom)
                        count += 1
            logger.info(f"  ✓ Listings persisted: {suburb_name} — {count}")

    logger.info("Step 4: Spatial enrichment (station distance only for test)")
    from src.enrichment.station_distance import load_stations_for_city
    load_stations_for_city(cfg, "sydney")
    from src.enrichment.station_distance import run_station_enrichment
    run_station_enrichment(cfg)

    logger.info("Step 5: Scoring engine")
    from src.scoring.engine import run_scoring_engine
    run_scoring_engine(cfg)

    logger.info("Step 6: Excel workbook")
    from src.outputs.excel_generator import generate_excel_workbook
    excel_path = generate_excel_workbook(cfg)
    logger.info(f"  ✓ Excel: {excel_path}")

    logger.info("Step 7: HTML dashboard")
    from src.outputs.dashboard_generator import generate_html_dashboard
    dash_path = generate_html_dashboard(cfg)
    logger.info(f"  ✓ Dashboard: {dash_path}")

    logger.info("=== TEST RUN COMPLETE ===")
    logger.info(f"Outputs in: {cfg.output_dir}")

    with get_session() as session:
        from src.database.models import Suburb, Listing, SuburbScore
        suburb_count = session.query(Suburb).count()
        listing_count = session.query(Listing).count()
        score_count = session.query(SuburbScore).count()
        logger.info(f"DB summary: {suburb_count} suburbs | {listing_count} listings | {score_count} scores")

    cfg.cities = original_cities


if __name__ == "__main__":
    main()
