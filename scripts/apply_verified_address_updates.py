#!/usr/bin/env python3
"""Apply verified address updates to local court JSON files."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
SOURCES_DIR = BASE_DIR / "docassemble" / "MACourts" / "data" / "sources"
MANUAL_PATH = SOURCES_DIR / "court_address_manual_verifications.json"

MASSGOV_JSONAPI = "https://www.mass.gov/jsonapi/node/location"


@dataclass
class ParsedAddress:
    building: str = ""
    address_line1: str = ""
    unit: str = ""


def normalize_text(value: str) -> str:
    value = value or ""
    value = value.strip().lower()
    value = re.sub(r"[\.,]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value


def parse_verified_address(address1: str, existing_unit: str = "") -> ParsedAddress:
    parts = [part.strip() for part in address1.split(",") if part.strip()]
    parsed = ParsedAddress(unit=existing_unit)

    if not parts:
        return parsed

    if len(parts) == 1:
        parsed.address_line1 = parts[0]
        return parsed

    if "courthouse" in parts[0].lower() and len(parts) >= 2:
        parsed.building = parts[0]
        parsed.address_line1 = parts[1]
        if len(parts) >= 3:
            parsed.unit = parts[2]
        return parsed

    parsed.address_line1 = parts[0]
    if len(parts) >= 2:
        parsed.unit = parts[1]

    if len(parts) > 2:
        remainder = ", ".join(parts[1:])
        parsed.unit = remainder

    return parsed


def build_orig_address(
    building: str,
    address_line1: str,
    unit: str,
    address2: str,
    city: str,
    state: str,
    zip_code: str,
) -> str:
    parts = []
    if building:
        parts.append(building)
    if address_line1:
        parts.append(address_line1)
    if unit:
        parts.append(unit)
    if address2:
        parts.append(address2)
    line = ", ".join(parts)
    city_state = ", ".join(part for part in [city, state] if part)
    if zip_code:
        city_state = f"{city_state} {zip_code}" if city_state else zip_code
    if city_state:
        return f"{line}, {city_state}" if line else city_state
    return line


def fetch_geofield(court_name: str, address_line1: str) -> Optional[Dict[str, float]]:
    params: Dict[str, str] = {
        "filter[title][operator]": "CONTAINS",
        "filter[title][value]": court_name,
        "page[limit]": "1",
        "include": "field_ref_contact_info,field_ref_contact_info.field_ref_address,"
        "field_ref_contact_info_1,field_ref_contact_info_1.field_ref_address",
    }
    resp = requests.get(MASSGOV_JSONAPI, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    addresses = [
        inc
        for inc in payload.get("included", [])
        if inc.get("type") == "paragraph--address"
    ]
    if not addresses:
        return None

    target_norm = normalize_text(address_line1)
    for addr in addresses:
        addr_data = addr.get("attributes", {}).get("field_address_address", {})
        addr_line1 = addr_data.get("address_line1", "")
        if normalize_text(addr_line1) == target_norm:
            geo = addr.get("attributes", {}).get("field_geofield")
            if geo and geo.get("lat") and geo.get("lon"):
                return {"lat": float(geo["lat"]), "lon": float(geo["lon"])}

    if len(addresses) == 1:
        geo = addresses[0].get("attributes", {}).get("field_geofield")
        if geo and geo.get("lat") and geo.get("lon"):
            return {"lat": float(geo["lat"]), "lon": float(geo["lon"])}

    return None


def main() -> None:
    manual = json.loads(MANUAL_PATH.read_text())

    files_cache: Dict[str, List[dict]] = {}

    for key, info in manual.items():
        if info.get("final_action") != "update_local_from_verified":
            continue

        source_file, court_name, court_city = key.split("::", 2)
        file_path = SOURCES_DIR / source_file
        if source_file not in files_cache:
            files_cache[source_file] = json.loads(file_path.read_text())

        entries = [
            e
            for e in files_cache[source_file]
            if e.get("name") == court_name
            and e.get("address", {}).get("city", "").lower() == court_city.lower()
        ]
        if not entries:
            raise RuntimeError(f"No entry found for {key}")
        if len(entries) > 1:
            raise RuntimeError(f"Multiple entries found for {key}")

        entry = entries[0]
        addr = entry.get("address", {})

        verified_address1 = info.get("verified_address1", "")
        verified_address2 = info.get("verified_address2", "")
        verified_city = info.get("verified_city", "")
        verified_state = info.get("verified_state", "")
        verified_zip = info.get("verified_zip", "")

        parsed = parse_verified_address(verified_address1, addr.get("unit", ""))
        if parsed.address_line1:
            addr["address"] = parsed.address_line1
        if parsed.unit:
            addr["unit"] = parsed.unit
        elif "unit" in addr:
            addr.pop("unit", None)

        addr["city"] = verified_city or addr.get("city", "")
        addr["state"] = verified_state or addr.get("state", "")
        addr["zip"] = verified_zip or addr.get("zip", "")

        has_po_box = bool(verified_address2) or entry.get("has_po_box")
        entry["has_po_box"] = bool(has_po_box)

        addr["orig_address"] = build_orig_address(
            parsed.building,
            addr.get("address", ""),
            addr.get("unit", ""),
            verified_address2,
            addr.get("city", ""),
            addr.get("state", ""),
            addr.get("zip", ""),
        )

        entry["address"] = addr

        try:
            geo = fetch_geofield(court_name, addr.get("address", ""))
        except requests.RequestException:
            geo = None
        if geo:
            entry["location"] = {
                "latitude": geo["lat"],
                "longitude": geo["lon"],
            }

    for source_file, data in files_cache.items():
        file_path = SOURCES_DIR / source_file
        file_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
