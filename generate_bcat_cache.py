"""
generate_bcat_cache.py — SafeHaven NC utility
──────────────────────────────────────────────
One-time script: pulls all NC county BCAT records from the live FEMA
GeoPlatform service and writes nc_bcat_statewide.json next to this file.

Run before first deploy (or after a major NC building-code adoption cycle):

    pip install httpx
    python generate_bcat_cache.py

The output file is consumed as the startup fallback by main.py whenever
the live FEMA API is unreachable.
"""

import json
import sys
from pathlib import Path

import httpx

FEMA_BCAT_URL = (
    "https://gis.fema.gov/arcgis/rest/services/FEMA/BCAT_County/MapServer/0/query"
)
OUT_PATH = Path(__file__).parent / "nc_bcat_statewide.json"
TIMEOUT = 20.0  # longer timeout acceptable for a one-shot refresh


def resistant_label(raw) -> str:
    return (
        "Resistant"
        if str(raw).strip().lower() in ("yes", "1", "true", "resistant", "y")
        else "Not Resistant"
    )


def main() -> None:
    params = {
        "where": "STATE = 'NC'",
        "outFields": (
            "COUNTY,COUNTY_FIPS,WIND_RESISTANT,FLOOD_RESISTANT,"
            "CODE_EDITION,BCAT_STATUS"
        ),
        "returnGeometry": "false",
        "resultRecordCount": 200,   # NC has 100 counties; 200 gives headroom
        "f": "json",
    }

    print(f"Querying FEMA BCAT service for all NC counties …")
    with httpx.Client(timeout=TIMEOUT) as client:
        resp = client.get(FEMA_BCAT_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features", [])
    if not features:
        print("ERROR: No features returned. Check the FEMA service URL or query params.")
        sys.exit(1)

    records = []
    skipped = 0
    for feat in features:
        attrs = feat.get("attributes", {})
        fips = str(attrs.get("COUNTY_FIPS") or "").strip().zfill(5)
        if len(fips) != 5 or fips == "00000":
            skipped += 1
            continue
        records.append(
            {
                "county_fips": fips,
                "county_name": str(attrs.get("COUNTY") or "").strip(),
                "bcat_wind_resistance": resistant_label(attrs.get("WIND_RESISTANT", "no")),
                "bcat_flood_resistance": resistant_label(attrs.get("FLOOD_RESISTANT", "no")),
                "building_code_era": str(
                    attrs.get("CODE_EDITION") or "Standard NC Code"
                ).strip(),
                "bcat_status": str(attrs.get("BCAT_STATUS") or "").strip(),
            }
        )

    records.sort(key=lambda r: r["county_fips"])

    OUT_PATH.write_text(
        json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(
        f"Done. {len(records)} counties written to {OUT_PATH} "
        f"({skipped} records skipped — missing FIPS)."
    )

    # Sanity check: NC should have exactly 100 counties.
    if len(records) != 100:
        print(
            f"WARNING: expected 100 NC counties, got {len(records)}. "
            "Inspect the output and the FEMA service for completeness."
        )


if __name__ == "__main__":
    main()
