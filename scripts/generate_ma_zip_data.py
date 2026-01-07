#!/usr/bin/env python3
import json
import math
from pathlib import Path

import pgeocode


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    output_path = repo_root / "docassemble" / "MACourts" / "data" / "sources" / "ma_zip_codes.json"

    nomi = pgeocode.Nominatim("us")
    data = nomi._data
    ma_rows = data[data["state_code"] == "MA"].copy()

    def normalize_value(value):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    records = {}
    for row in ma_rows.itertuples(index=False):
        postal_code = getattr(row, "postal_code", None)
        if not postal_code or str(postal_code) == "nan":
            continue
        postal_code = str(postal_code)
        records[postal_code] = {
            "place_name": normalize_value(getattr(row, "place_name", None)),
            "county_name": normalize_value(getattr(row, "county_name", None)),
            "latitude": normalize_value(getattr(row, "latitude", None)),
            "longitude": normalize_value(getattr(row, "longitude", None)),
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as outfile:
        json.dump({k: records[k] for k in sorted(records)}, outfile, indent=2, sort_keys=False)
        outfile.write("\n")

    print(f"Wrote {len(records)} MA zip code records to {output_path}")


if __name__ == "__main__":
    main()
