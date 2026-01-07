#!/usr/bin/env python3
"""Validate local court addresses against mass.gov location data.

Fetches mass.gov location nodes via JSON:API, extracts address data, and
compares to the local JSON source files. Outputs a CSV report.
"""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
SOURCES_DIR = BASE_DIR / "docassemble" / "MACourts" / "data" / "sources"
OUTPUT_CSV = SOURCES_DIR / "court_address_validation.csv"
CACHE_JSON = SOURCES_DIR / ".cache_massgov_locations.json"
MANUAL_VERIFICATIONS = SOURCES_DIR / "court_address_manual_verifications.json"

MASSGOV_JSONAPI = "https://www.mass.gov/jsonapi/node/location"

JSON_FILES = sorted(
    p
    for p in SOURCES_DIR.glob("*.json")
    if p.is_file()
    and not p.name.startswith(".")
    and p.name not in {"court_address_manual_verifications.json"}
)

NAME_STOPWORDS = {
    "the",
    "department",
    "division",
    "dept",
    "court",
    "courthouse",
    "municipal",
    "probate",
    "family",
}

ADDRESS_ABBREVIATIONS = {
    "street": "st",
    "road": "rd",
    "avenue": "ave",
    "boulevard": "blvd",
    "drive": "dr",
    "court": "ct",
    "place": "pl",
    "square": "sq",
    "lane": "ln",
    "highway": "hwy",
    "route": "rt",
    "parkway": "pkwy",
    "circle": "cir",
    "center": "ctr",
    "mount": "mt",
}


@dataclass
class LocalCourt:
    source_file: str
    name: str
    court_code: str
    tyler_code: str
    phone: str
    fax: str
    has_po_box: bool
    address1: str
    city: str
    state: str
    zip: str
    county: str
    orig_address: str


@dataclass
class MassGovLocation:
    title: str
    location_id: str
    path_alias: Optional[str]
    addresses: List[Dict[str, str]] = field(default_factory=list)
    cities: set = field(default_factory=set)

    @property
    def url(self) -> str:
        if self.path_alias:
            return f"https://www.mass.gov{self.path_alias}"
        return ""


def normalize_text(value: str) -> str:
    value = value or ""
    value = value.strip().lower()
    value = re.sub(r"[\.,]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_name(value: str) -> str:
    value = normalize_text(value)
    tokens = [t for t in value.split() if t not in NAME_STOPWORDS]
    return " ".join(tokens)


def normalize_address(value: str) -> str:
    value = normalize_text(value)
    tokens = []
    for token in value.split():
        token = ADDRESS_ABBREVIATIONS.get(token, token)
        tokens.append(token)
    return " ".join(tokens)


def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def load_local_courts() -> List[LocalCourt]:
    courts: List[LocalCourt] = []
    for path in JSON_FILES:
        with path.open() as handle:
            data = json.load(handle)
        for entry in data:
            address = entry.get("address", {})
            courts.append(
                LocalCourt(
                    source_file=path.name,
                    name=entry.get("name", ""),
                    court_code=entry.get("court_code", ""),
                    tyler_code=entry.get("tyler_code", ""),
                    phone=entry.get("phone", ""),
                    fax=entry.get("fax", ""),
                    has_po_box=bool(entry.get("has_po_box")),
                    address1=address.get("address", ""),
                    city=address.get("city", ""),
                    state=address.get("state", ""),
                    zip=address.get("zip", ""),
                    county=address.get("county", ""),
                    orig_address=address.get("orig_address", ""),
                )
            )
    return courts


def fetch_massgov_locations(force_refresh: bool = False) -> List[MassGovLocation]:
    if CACHE_JSON.exists() and not force_refresh:
        with CACHE_JSON.open() as handle:
            cached = json.load(handle)
        return [
            MassGovLocation(
                title=item["title"],
                location_id=item["location_id"],
                path_alias=item.get("path_alias"),
                addresses=item.get("addresses", []),
                cities=set(item.get("cities", [])),
            )
            for item in cached
        ]

    session = requests.Session()
    locations: List[MassGovLocation] = []
    offset = 0
    limit = 20

    while True:
        params: Dict[str, str] = {
            "filter[title][operator]": "CONTAINS",
            "filter[title][value]": "Court",
            "page[limit]": str(limit),
            "page[offset]": str(offset),
            "include": "field_ref_contact_info,field_ref_contact_info.field_ref_address,"
            "field_ref_contact_info_1,field_ref_contact_info_1.field_ref_address",
        }
        resp = session.get(MASSGOV_JSONAPI, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        included = payload.get("included", [])

        address_by_id: Dict[str, Dict[str, str]] = {}
        contact_to_addresses: Dict[str, List[str]] = {}

        for inc in included:
            inc_type = inc.get("type")
            inc_id = inc.get("id")
            if inc_type == "paragraph--address":
                address = inc.get("attributes", {}).get("field_address_address") or {}
                address_by_id[inc_id] = {
                    "address_line1": address.get("address_line1", "") or "",
                    "address_line2": address.get("address_line2", "") or "",
                    "city": address.get("locality", "") or "",
                    "state": address.get("administrative_area", "") or "",
                    "postal_code": address.get("postal_code", "") or "",
                    "country": address.get("country_code", "") or "",
                }
            elif inc_type == "node--contact_information":
                rel = (
                    inc.get("relationships", {})
                    .get("field_ref_address", {})
                    .get("data")
                )
                address_ids: List[str] = []
                if isinstance(rel, list):
                    address_ids = [r["id"] for r in rel]
                elif isinstance(rel, dict):
                    address_ids = [rel["id"]]
                contact_to_addresses[inc_id] = address_ids

        for loc in payload.get("data", []):
            loc_id = loc.get("id")
            attrs = loc.get("attributes", {})
            title = attrs.get("title", "")
            path_alias = (attrs.get("path") or {}).get("alias")

            contact_ids: List[str] = []
            for rel_key in ("field_ref_contact_info", "field_ref_contact_info_1"):
                rel_data = loc.get("relationships", {}).get(rel_key, {}).get("data")
                if isinstance(rel_data, list):
                    contact_ids.extend([r["id"] for r in rel_data])
                elif isinstance(rel_data, dict):
                    contact_ids.append(rel_data["id"])

            addresses: List[Dict[str, str]] = []
            seen = set()
            for contact_id in contact_ids:
                for addr_id in contact_to_addresses.get(contact_id, []):
                    addr = address_by_id.get(addr_id)
                    if not addr:
                        continue
                    key = (
                        addr.get("address_line1", ""),
                        addr.get("address_line2", ""),
                        addr.get("city", ""),
                        addr.get("postal_code", ""),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    addresses.append(addr)

            cities = {
                normalize_text(a.get("city", "")) for a in addresses if a.get("city")
            }
            locations.append(
                MassGovLocation(
                    title=title,
                    location_id=loc_id,
                    path_alias=path_alias,
                    addresses=addresses,
                    cities=cities,
                )
            )

        if "next" not in payload.get("links", {}):
            break

        offset += limit
        time.sleep(0.25)

    CACHE_JSON.write_text(
        json.dumps(
            [
                {
                    "title": loc.title,
                    "location_id": loc.location_id,
                    "path_alias": loc.path_alias,
                    "addresses": loc.addresses,
                    "cities": sorted(loc.cities),
                }
                for loc in locations
            ],
            indent=2,
        )
    )

    return locations


def candidate_locations(
    local_city: str, locations: Iterable[MassGovLocation]
) -> List[MassGovLocation]:
    normalized_city = normalize_text(local_city)
    city_matches = [
        loc for loc in locations if normalized_city and normalized_city in loc.cities
    ]
    return city_matches or list(locations)


def find_best_match(
    local: LocalCourt, locations: List[MassGovLocation]
) -> Tuple[Optional[MassGovLocation], float, bool, bool]:
    local_name_norm = normalize_name(local.name)
    local_addr_norm = normalize_address(local.address1)
    local_city_norm = normalize_text(local.city)

    best = None
    best_score = 0.0
    best_addr_match = False
    best_city_match = False

    for loc in locations:
        title_norm = normalize_name(loc.title)
        score = name_similarity(local_name_norm, title_norm)

        city_match = False
        if local_city_norm and local_city_norm in loc.cities:
            score += 0.1
            city_match = True

        addr_match = False
        if local_addr_norm:
            for addr in loc.addresses:
                addr_norm = normalize_address(addr.get("address_line1", ""))
                if addr_norm and addr_norm == local_addr_norm:
                    score += 0.2
                    addr_match = True
                    break
                if (
                    local.orig_address
                    and addr_norm
                    and addr_norm in normalize_address(local.orig_address)
                ):
                    score += 0.15
                    addr_match = True
                    break

        if score > best_score:
            best = loc
            best_score = score
            best_addr_match = addr_match
            best_city_match = city_match

    if best_score < 0.6:
        return None, 0.0, False, False

    return best, best_score, best_addr_match, best_city_match


def pick_primary_address(location: MassGovLocation) -> Dict[str, str]:
    if location.addresses:
        return location.addresses[0]
    return {
        "address_line1": "",
        "address_line2": "",
        "city": "",
        "state": "",
        "postal_code": "",
    }


def recommend_action(
    local: LocalCourt,
    match: Optional[MassGovLocation],
    score: float,
    addr_match: bool,
    city_match: bool,
) -> Tuple[str, str, str]:
    if match is None:
        return "review", "needs_manual", "No mass.gov location match"

    if addr_match:
        return "no_change", "local", "Address matches mass.gov"

    if score >= 0.8 and city_match and match.addresses:
        return "update_local_from_massgov", "massgov", "High-confidence name/city match"

    return "verify_web", "needs_manual", "Name match without address confirmation"


def write_report(
    local_courts: List[LocalCourt], locations: List[MassGovLocation]
) -> None:
    manual_verifications: Dict[str, Dict[str, str]] = {}
    if MANUAL_VERIFICATIONS.exists():
        manual_verifications = json.loads(MANUAL_VERIFICATIONS.read_text())

    fieldnames = [
        "source_file",
        "court_name",
        "court_code",
        "tyler_code",
        "phone",
        "fax",
        "has_po_box",
        "local_address1",
        "local_city",
        "local_state",
        "local_zip",
        "local_county",
        "local_orig_address",
        "massgov_name",
        "massgov_match_score",
        "massgov_address1",
        "massgov_address2",
        "massgov_city",
        "massgov_state",
        "massgov_zip",
        "massgov_url",
        "address_match",
        "city_match",
        "recommended_action",
        "preferred_source",
        "notes",
        "verified_address1",
        "verified_address2",
        "verified_city",
        "verified_state",
        "verified_zip",
        "verification_source_name",
        "verification_source_url",
        "verification_notes",
        "secondary_address1",
        "secondary_address2",
        "secondary_city",
        "secondary_state",
        "secondary_zip",
        "secondary_source_name",
        "secondary_source_url",
        "secondary_source_notes",
        "secondary_source_confidence",
        "final_action",
    ]

    with OUTPUT_CSV.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for local in local_courts:
            candidates = candidate_locations(local.city, locations)
            match, score, addr_match, city_match = find_best_match(local, candidates)
            primary = pick_primary_address(match) if match else {}
            action, preferred, note = recommend_action(
                local, match, score, addr_match, city_match
            )
            manual_key = f"{local.source_file}::{local.name}::{local.city}"
            manual = manual_verifications.get(manual_key, {})

            verified_address1 = manual.get("verified_address1", "")
            verified_address2 = manual.get("verified_address2", "")
            verified_city = manual.get("verified_city", "")
            verified_state = manual.get("verified_state", "")
            verified_zip = manual.get("verified_zip", "")
            verification_source_name = manual.get("verification_source_name", "")
            verification_source_url = manual.get("verification_source_url", "")
            verification_notes = manual.get("verification_notes", "")
            secondary_address1 = manual.get("secondary_address1", "")
            secondary_address2 = manual.get("secondary_address2", "")
            secondary_city = manual.get("secondary_city", "")
            secondary_state = manual.get("secondary_state", "")
            secondary_zip = manual.get("secondary_zip", "")
            secondary_source_name = manual.get("secondary_source_name", "")
            secondary_source_url = manual.get("secondary_source_url", "")
            secondary_source_notes = manual.get("secondary_source_notes", "")
            secondary_source_confidence = manual.get("secondary_source_confidence", "")
            final_action = manual.get("final_action", "")

            if manual.get("preferred_source"):
                preferred = manual["preferred_source"]
            if manual.get("final_action"):
                action = manual["final_action"]
            if verification_notes:
                note = verification_notes

            writer.writerow(
                {
                    "source_file": local.source_file,
                    "court_name": local.name,
                    "court_code": local.court_code,
                    "tyler_code": local.tyler_code,
                    "phone": local.phone,
                    "fax": local.fax,
                    "has_po_box": local.has_po_box,
                    "local_address1": local.address1,
                    "local_city": local.city,
                    "local_state": local.state,
                    "local_zip": local.zip,
                    "local_county": local.county,
                    "local_orig_address": local.orig_address,
                    "massgov_name": match.title if match else "",
                    "massgov_match_score": f"{score:.2f}" if match else "",
                    "massgov_address1": primary.get("address_line1", ""),
                    "massgov_address2": primary.get("address_line2", ""),
                    "massgov_city": primary.get("city", ""),
                    "massgov_state": primary.get("state", ""),
                    "massgov_zip": primary.get("postal_code", ""),
                    "massgov_url": match.url if match else "",
                    "address_match": addr_match,
                    "city_match": city_match,
                    "recommended_action": action,
                    "preferred_source": preferred,
                    "notes": note,
                    "verified_address1": verified_address1,
                    "verified_address2": verified_address2,
                    "verified_city": verified_city,
                    "verified_state": verified_state,
                    "verified_zip": verified_zip,
                    "verification_source_name": verification_source_name,
                    "verification_source_url": verification_source_url,
                    "verification_notes": verification_notes,
                    "secondary_address1": secondary_address1,
                    "secondary_address2": secondary_address2,
                    "secondary_city": secondary_city,
                    "secondary_state": secondary_state,
                    "secondary_zip": secondary_zip,
                    "secondary_source_name": secondary_source_name,
                    "secondary_source_url": secondary_source_url,
                    "secondary_source_notes": secondary_source_notes,
                    "secondary_source_confidence": secondary_source_confidence,
                    "final_action": final_action,
                }
            )


def main() -> None:
    local_courts = load_local_courts()
    locations = fetch_massgov_locations()
    write_report(local_courts, locations)
    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
