"""
Council agenda scraper — monthly scrape of all in-scope LGA websites.
Flags agendas containing planning-sensitive keywords.
Uses Claude API to summarise flagged items.
"""
from __future__ import annotations
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import anthropic

from ..config import Config
from ..database.session import get_session
from ..database.models import CouncilAlert, Suburb

COUNCIL_KEYWORDS = [
    "rezoning", "upzoning", "heritage listing", "height limit",
    "height limit amendment", "masterplan", "master plan",
    "TOD", "transit-oriented development", "urban renewal",
    "infrastructure contribution", "infrastructure levy",
    "planning proposal", "state environmental planning policy",
    "SEPP", "local environmental plan", "LEP amendment",
]

HIGH_SIGNIFICANCE_KEYWORDS = [
    "rezoning", "upzoning", "TOD", "transit-oriented development",
    "urban renewal", "masterplan", "master plan",
]

NSW_COUNCILS = {
    "City of Parramatta": "https://www.cityofparramatta.nsw.gov.au/about-council/council-meetings/business-papers",
    "Canterbury-Bankstown Council": "https://www.cbcity.nsw.gov.au/council/meetings",
    "Blacktown City Council": "https://www.blacktown.nsw.gov.au/Council/Meetings",
    "Penrith City Council": "https://www.penrithcity.nsw.gov.au/Council/council-meetings",
    "Liverpool City Council": "https://www.liverpool.nsw.gov.au/Council/Meetings",
    "Georges River Council": "https://www.georgesriver.nsw.gov.au/Council/Meetings",
    "City of Ryde": "https://www.ryde.nsw.gov.au/Council/Meetings",
    "Hornsby Shire Council": "https://www.hornsby.nsw.gov.au/council/council-meetings",
    "Willoughby City Council": "https://www.willoughby.nsw.gov.au/Council/Council-Meetings",
    "Sutherland Shire": "https://www.sutherlandshire.nsw.gov.au/Council/Council-Meetings",
    "Cumberland Council": "https://www.cumberland.nsw.gov.au/council/council-meetings",
    "Northern Beaches Council": "https://www.northernbeaches.nsw.gov.au/council/meeting-agendas-minutes",
    "Fairfield City Council": "https://www.fairfieldcity.nsw.gov.au/Council/Meetings",
    "Campbelltown City Council": "https://www.campbelltown.nsw.gov.au/Council/Council-Meetings",
    "Burwood Council": "https://www.burwood.nsw.gov.au/council/meetings-minutes",
    "Strathfield Council": "https://www.strathfield.nsw.gov.au/council/council-meetings",
}

VIC_COUNCILS = {
    "City of Maribyrnong": "https://www.maribyrnong.vic.gov.au/council/meetings-and-agendas",
    "City of Brimbank": "https://www.brimbank.vic.gov.au/council-and-democracy/meetings-and-decisions",
    "City of Wyndham": "https://www.wyndham.vic.gov.au/council/meetings-and-agendas",
    "City of Greater Dandenong": "https://www.greaterdandenong.vic.gov.au/council-meetings",
    "City of Whitehorse": "https://www.whitehorse.vic.gov.au/council/meetings-and-decisions",
    "City of Maroondah": "https://www.maroondah.vic.gov.au/council/meetings-agendas-minutes",
    "City of Darebin": "https://www.darebin.vic.gov.au/Council/Meetings-and-agendas",
    "City of Moreland": "https://www.moreland.vic.gov.au/council/council-meetings/",
    "City of Frankston": "https://www.frankston.vic.gov.au/Council/Meetings-and-Agendas",
    "City of Casey": "https://www.casey.vic.gov.au/council/meetings-decisions",
    "City of Whittlesea": "https://www.whittlesea.vic.gov.au/council/meetings-agendas",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
}


def _extract_agenda_text(url: str) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return text[:50000]
    except Exception as e:
        logger.debug(f"Failed to fetch agenda {url}: {e}")
        return ""


def _find_agenda_links(council_url: str) -> List[str]:
    try:
        resp = requests.get(council_url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            text = a.text.lower()
            if any(kw in href or kw in text for kw in
                   ["agenda", "business-paper", "business paper", "meeting"]):
                full_url = a["href"]
                if full_url.startswith("/"):
                    base = "/".join(council_url.split("/")[:3])
                    full_url = base + full_url
                elif not full_url.startswith("http"):
                    full_url = council_url.rstrip("/") + "/" + full_url
                links.append(full_url)
        return links[:5]
    except Exception as e:
        logger.debug(f"Failed to get agenda links from {council_url}: {e}")
        return []


def _match_keywords(text: str) -> List[str]:
    matched = []
    text_lower = text.lower()
    for kw in COUNCIL_KEYWORDS:
        if kw.lower() in text_lower:
            matched.append(kw)
    return matched


def _determine_significance(keywords: List[str]) -> str:
    for kw in keywords:
        if any(hk.lower() in kw.lower() for hk in HIGH_SIGNIFICANCE_KEYWORDS):
            return "HIGH"
    return "MEDIUM" if keywords else "LOW"


def _extract_agenda_items(text: str, keywords: List[str]) -> str:
    sentences = re.split(r'[.!?]\s+', text)
    relevant = [s for s in sentences
                if any(kw.lower() in s.lower() for kw in keywords)]
    return " | ".join(relevant[:10])[:2000]


def _summarise_with_claude(api_key: str, council: str, agenda_text: str,
                            keywords: List[str]) -> Optional[str]:
    if not api_key:
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = (
            f"You are a property investment analyst. A council agenda for {council} "
            f"contains these planning-related keywords: {', '.join(keywords)}.\n\n"
            f"Agenda excerpt:\n{agenda_text[:3000]}\n\n"
            f"Summarise in 2-3 sentences the specific planning items found, "
            f"their likely impact on local property values, and urgency for an investor."
        )
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text
    except Exception as e:
        logger.debug(f"Claude summarisation failed: {e}")
        return None


def _scrape_council(lga: str, url: str, city: str, api_key: str,
                    session) -> List[CouncilAlert]:
    alerts = []
    agenda_links = _find_agenda_links(url)
    if not agenda_links:
        agenda_links = [url]

    for agenda_url in agenda_links[:3]:
        time.sleep(1)
        text = _extract_agenda_text(agenda_url)
        if not text:
            continue

        keywords = _match_keywords(text)
        if not keywords:
            continue

        significance = _determine_significance(keywords)
        agenda_item = _extract_agenda_items(text, keywords)

        summary = None
        if significance == "HIGH" and api_key:
            summary = _summarise_with_claude(api_key, lga, agenda_item, keywords)

        suburb = session.query(Suburb).filter(
            Suburb.city == city,
            Suburb.lga == lga,
        ).first()

        alert = CouncilAlert(
            suburb_id=suburb.id if suburb else None,
            city=city,
            lga=lga,
            meeting_date=datetime.utcnow(),
            agenda_item=agenda_item,
            keywords_matched=json.dumps(keywords),
            significance=significance,
            source_url=agenda_url,
            raw_text=text[:5000],
            claude_summary=summary,
        )
        alerts.append(alert)
        logger.info(f"Council alert [{significance}] — {lga}: {', '.join(keywords[:3])}")

    return alerts


def run_council_scrape(cfg: Config) -> None:
    logger.info("Starting council agenda scrape")
    api_key = cfg.api_keys.claude_api_key
    total_alerts = 0

    council_sets = []
    if "sydney" in cfg.cities:
        council_sets.append(("sydney", NSW_COUNCILS))
    if "melbourne" in cfg.cities:
        council_sets.append(("melbourne", VIC_COUNCILS))

    for city, councils in council_sets:
        for lga, url in councils.items():
            logger.debug(f"Scraping council: {lga}")
            try:
                with get_session() as session:
                    alerts = _scrape_council(lga, url, city, api_key, session)
                    for alert in alerts:
                        session.add(alert)
                    total_alerts += len(alerts)
            except Exception as e:
                logger.error(f"Error scraping {lga}: {e}")
            time.sleep(2)

    logger.info(f"Council scrape complete — {total_alerts} alerts generated")
