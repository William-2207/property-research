"""
Scrapes infrastructure pipeline data for NSW (Infrastructure NSW) and VIC (Infrastructure Victoria).
Classifies and stores projects within 5km of qualifying suburbs.
"""
from __future__ import annotations
import re
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Config
from ..database.session import get_session
from ..database.models import InfrastructureProject, Suburb
from ..enrichment.station_distance import haversine_m


INFRA_NSW_URL = "https://www.infrastructure.nsw.gov.au/projects/"
INFRA_VIC_URL = "https://www.infrastructurevictoria.com.au/projects/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
}

PROJECT_TYPES = [
    "transport", "road", "rail", "metro", "hospital", "school",
    "park", "sewer", "water", "energy", "social", "housing",
    "commercial", "stadium", "library", "community", "bridge",
]


def classify_project_type(text: str) -> str:
    text_lower = text.lower()
    for ptype in PROJECT_TYPES:
        if ptype in text_lower:
            return ptype.title()
    return "Other"


def _parse_value(text: str) -> Optional[float]:
    """Parse dollar value string to float."""
    text = text.replace(",", "").replace("$", "").strip()
    multipliers = {"billion": 1e9, "million": 1e6, "m": 1e6, "b": 1e9, "k": 1e3}
    for suffix, mult in multipliers.items():
        if suffix in text.lower():
            nums = re.findall(r"[\d.]+", text)
            if nums:
                return float(nums[0]) * mult
    nums = re.findall(r"[\d.]+", text)
    if nums:
        return float(nums[0])
    return None


def _parse_date(text: str) -> Optional[datetime]:
    for fmt in ["%Y", "%B %Y", "%b %Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    year_match = re.search(r"\b(202[0-9]|203[0-9])\b", text)
    if year_match:
        try:
            return datetime(int(year_match.group(1)), 1, 1)
        except ValueError:
            pass
    return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _scrape_infra_nsw() -> List[Dict]:
    projects = []
    try:
        resp = requests.get(INFRA_NSW_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        cards = soup.select(".project-card, .project-item, [data-project], article.project")
        for card in cards:
            name_el = card.select_one("h2, h3, h4, .project-title, .project-name")
            if not name_el:
                continue
            name = name_el.text.strip()

            value_el = card.select_one(".project-value, .value, [data-value]")
            value_text = value_el.text if value_el else ""
            value = _parse_value(value_text)

            status_el = card.select_one(".project-status, .status")
            status = status_el.text.strip() if status_el else "Unknown"

            date_el = card.select_one(".completion-date, .expected-date, .timeline")
            expected_date = _parse_date(date_el.text) if date_el else None

            location_el = card.select_one(".location, .suburb, .lga")
            location = location_el.text.strip() if location_el else ""

            link_el = card.select_one("a[href]")
            url = f"https://www.infrastructure.nsw.gov.au{link_el['href']}" if link_el else INFRA_NSW_URL

            projects.append({
                "project_name": name,
                "project_type": classify_project_type(name),
                "estimated_value": value,
                "status": status,
                "expected_completion_date": expected_date,
                "location_hint": location,
                "source_url": url,
                "city": "sydney",
            })

        if not projects:
            api_resp = requests.get(
                "https://www.infrastructure.nsw.gov.au/api/projects",
                headers=HEADERS, timeout=30
            )
            if api_resp.status_code == 200:
                data = api_resp.json()
                items = data if isinstance(data, list) else data.get("projects", data.get("items", []))
                for item in items:
                    projects.append({
                        "project_name": item.get("name", item.get("title", "")),
                        "project_type": classify_project_type(item.get("type", "")),
                        "estimated_value": _parse_value(str(item.get("value", item.get("cost", "")))),
                        "status": item.get("status", "Unknown"),
                        "expected_completion_date": _parse_date(str(item.get("completion", ""))),
                        "location_hint": item.get("suburb", item.get("location", "")),
                        "source_url": INFRA_NSW_URL,
                        "city": "sydney",
                    })

    except Exception as e:
        logger.error(f"Error scraping Infrastructure NSW: {e}")

    logger.info(f"Infrastructure NSW: found {len(projects)} projects")
    return projects


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _scrape_infra_vic() -> List[Dict]:
    projects = []
    try:
        resp = requests.get(INFRA_VIC_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        cards = soup.select(".project-card, .project-item, article.project, .project")
        for card in cards:
            name_el = card.select_one("h2, h3, h4, .project-title")
            if not name_el:
                continue
            name = name_el.text.strip()

            value_el = card.select_one(".cost, .value, .budget")
            value = _parse_value(value_el.text) if value_el else None

            status_el = card.select_one(".status, .project-status")
            status = status_el.text.strip() if status_el else "Unknown"

            date_el = card.select_one(".date, .completion, .timeline")
            expected_date = _parse_date(date_el.text) if date_el else None

            location_el = card.select_one(".location, .suburb, .region")
            location = location_el.text.strip() if location_el else ""

            link_el = card.select_one("a[href]")
            url = (f"https://www.infrastructurevictoria.com.au{link_el['href']}"
                   if link_el and link_el['href'].startswith('/') else
                   link_el['href'] if link_el else INFRA_VIC_URL)

            projects.append({
                "project_name": name,
                "project_type": classify_project_type(name),
                "estimated_value": value,
                "status": status,
                "expected_completion_date": expected_date,
                "location_hint": location,
                "source_url": url,
                "city": "melbourne",
            })

    except Exception as e:
        logger.error(f"Error scraping Infrastructure Victoria: {e}")

    logger.info(f"Infrastructure VIC: found {len(projects)} projects")
    return projects


def _geocode_location(location_hint: str, city: str) -> Optional[Tuple[float, float]]:
    """Geocode a location hint using Nominatim."""
    if not location_hint:
        return None
    state = "NSW" if city == "sydney" else "VIC"
    query = f"{location_hint}, {state}, Australia"
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1},
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


def _match_projects_to_suburbs(projects: List[Dict], session, radius_m: float = 5000.0) -> None:
    suburbs = session.query(Suburb).all()

    for project in projects:
        location_hint = project.get("location_hint", "")
        city = project.get("city", "")

        matching_suburbs = [s for s in suburbs
                            if s.city == city and
                            (location_hint.lower() in s.name.lower() or
                             s.name.lower() in location_hint.lower())]

        for suburb in matching_suburbs:
            if not suburb.lat or not suburb.lon:
                continue
            distance = radius_m / 2

            existing = session.query(InfrastructureProject).filter_by(
                project_name=project["project_name"],
                suburb_id=suburb.id,
            ).first()

            if not existing:
                proj = InfrastructureProject(
                    suburb_id=suburb.id,
                    city=city,
                    project_name=project["project_name"][:500],
                    project_type=project.get("project_type", "Other"),
                    suburb_name=suburb.name,
                    lga=suburb.lga or "",
                    distance_to_suburb_centre_m=distance,
                    estimated_value=project.get("estimated_value"),
                    status=project.get("status", "Unknown"),
                    expected_completion_date=project.get("expected_completion_date"),
                    source_url=project.get("source_url", ""),
                )
                session.add(proj)


from typing import Tuple


def run_infrastructure_scrape(cfg: Config) -> None:
    logger.info("Starting infrastructure project scrape")
    all_projects = []

    if "sydney" in cfg.cities:
        nsw_projects = _scrape_infra_nsw()
        all_projects.extend(nsw_projects)
        time.sleep(2)

    if "melbourne" in cfg.cities:
        vic_projects = _scrape_infra_vic()
        all_projects.extend(vic_projects)

    with get_session() as session:
        _match_projects_to_suburbs(all_projects, session)

    logger.info(f"Infrastructure scrape complete — {len(all_projects)} projects processed")
