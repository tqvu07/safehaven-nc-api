"""
SafeHaven NC — Emergency Resilience and Adaptive Shelter Router
Carolina Data Challenge

Single-file FastAPI service providing:
  - Live wind-gust forecasting via Open-Meteo
  - FEMA BCAT-based county building code lookup (NC Triangle counties)
  - Structural vulnerability / predicted-damage scoring
  - Survivable-shelter filtering and routing (Haversine + Google Maps links)
"""

import math
from typing import Optional, List, Dict, Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SafeHaven NC",
    description="Emergency resilience and adaptive shelter router for the NC Triangle region.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

# FEMA BCAT county attributes for the NC Triangle counties.
# Each entry includes an approximate lat/lon bounding box used for
# coordinate-based matching (in addition to name-based matching).
COUNTY_BCAT_DB: Dict[str, Dict[str, Any]] = {
    "Orange": {
        "county_name": "Orange County",
        "county_fips": "37135",
        "bcat_wind_resistance": "Not Resistant",
        "bcat_flood_resistance": "Not Resistant",
        "building_code_era": "Legacy IRC/IBC",
        "bbox": {"lat_min": 35.86, "lat_max": 36.15, "lon_min": -79.24, "lon_max": -78.98},
    },
    "Durham": {
        "county_name": "Durham County",
        "county_fips": "37063",
        "bcat_wind_resistance": "Not Resistant",
        "bcat_flood_resistance": "Not Resistant",
        "building_code_era": "Standard NC Code",
        "bbox": {"lat_min": 35.86, "lat_max": 36.15, "lon_min": -78.98, "lon_max": -78.72},
    },
    "Wake": {
        "county_name": "Wake County",
        "county_fips": "37183",
        "bcat_wind_resistance": "Resistant",
        "bcat_flood_resistance": "Resistant",
        "building_code_era": "2018 NC State Residential Code",
        "bbox": {"lat_min": 35.55, "lat_max": 35.99, "lon_min": -78.90, "lon_max": -78.30},
    },
}

DEFAULT_COUNTY_KEY = "Orange"

# Verified emergency shelters with structural engineering wind survival limits.
SHELTERS: List[Dict[str, Any]] = [
    {
        "name": "East Chapel Hill High School",
        "county": "Orange",
        "max_wind_rating_mph": 120,
        "capacity": 450,
        "pet_friendly": True,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.9542,
        "lon": -79.0354,
    },
    {
        "name": "Smith Middle School",
        "county": "Orange",
        "max_wind_rating_mph": 110,
        "capacity": 350,
        "pet_friendly": False,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.9427,
        "lon": -79.0805,
    },
    {
        "name": "Durham County Memorial Stadium",
        "county": "Durham",
        "max_wind_rating_mph": 130,
        "capacity": 800,
        "pet_friendly": True,
        "ada_accessible": True,
        "generator": True,
        "lat": 36.0345,
        "lon": -78.8920,
    },
    {
        "name": "Southern Durham High School",
        "county": "Durham",
        "max_wind_rating_mph": 115,
        "capacity": 500,
        "pet_friendly": False,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.9320,
        "lon": -78.8576,
    },
    {
        "name": "PNC Arena / Carter-Finley Complex",
        "county": "Wake",
        "max_wind_rating_mph": 150,
        "capacity": 1500,
        "pet_friendly": True,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.8033,
        "lon": -78.7218,
        "notes": "Engineered Steel Frame",
    },
    {
        "name": "Southeast Raleigh Magnet High",
        "county": "Wake",
        "max_wind_rating_mph": 120,
        "capacity": 600,
        "pet_friendly": True,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.7533,
        "lon": -78.6012,
    },
    {
        "name": "Cary High School",
        "county": "Wake",
        "max_wind_rating_mph": 115,
        "capacity": 550,
        "pet_friendly": False,
        "ada_accessible": True,
        "generator": True,
        "lat": 35.7766,
        "lon": -78.7709,
    },
]

STRUCTURE_MULTIPLIERS = {
    "mobile_home": 2.2,
    "single_family": 1.0,
    "multi_family": 0.7,
}

# Baseline replacement-value assumptions per structure type, used for the
# predicted-damage estimate.
STRUCTURE_BASE_VALUE_USD = {
    "mobile_home": 80_000,
    "single_family": 300_000,
    "multi_family": 550_000,
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class EvaluateRequest(BaseModel):
    lat: float
    lon: float
    year_built: int
    structure_type: str = Field(..., description="mobile_home | single_family | multi_family")
    scenario_wind_mph: Optional[float] = None


class ShelterResult(BaseModel):
    name: str
    county: str
    max_wind_rating_mph: float
    capacity: int
    pet_friendly: bool
    ada_accessible: bool
    generator: bool
    distance_miles: float
    lat: float
    lon: float
    navigation_url: str


class EvaluateResponse(BaseModel):
    county_name: str
    county_fips: str
    bcat_wind_resistance: str
    bcat_flood_resistance: str
    building_code_era: str
    storm_wind_mph: float
    vulnerability_score: int
    predicted_damage_usd: float
    recommendation: str
    survivable_shelters: List[ShelterResult]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in miles."""
    r_miles = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r_miles * c


def match_county(lat: float, lon: float) -> Dict[str, Any]:
    """Match coordinates to a Triangle county by bounding box. Falls back to
    Orange County if no county boundary contains the point."""
    for key, county in COUNTY_BCAT_DB.items():
        bbox = county["bbox"]
        if (
            bbox["lat_min"] <= lat <= bbox["lat_max"]
            and bbox["lon_min"] <= lon <= bbox["lon_max"]
        ):
            return county
    return COUNTY_BCAT_DB[DEFAULT_COUNTY_KEY]


def match_county_by_name(name: str) -> Optional[Dict[str, Any]]:
    normalized = name.strip().lower()
    for key, county in COUNTY_BCAT_DB.items():
        if key.lower() == normalized or county["county_name"].lower() == normalized:
            return county
    return None


def fetch_peak_wind_gust(lat: float, lon: float) -> float:
    """Query Open-Meteo for the max wind gust (mph) over the next 72 hours."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,wind_gusts_10m",
        "forecast_days": 3,
        "wind_speed_unit": "mph",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        gusts = data.get("hourly", {}).get("wind_gusts_10m", [])
        if not gusts:
            raise ValueError("No wind gust data returned by Open-Meteo.")
        return float(max(gusts))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=502,
            detail=f"Failed to retrieve live weather forecast: {exc}",
        )


def calculate_vulnerability(
    year_built: int,
    structure_type: str,
    county: Dict[str, Any],
    storm_wind_mph: float,
) -> Dict[str, Any]:
    if structure_type not in STRUCTURE_MULTIPLIERS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid structure_type '{structure_type}'. "
                f"Must be one of: {list(STRUCTURE_MULTIPLIERS.keys())}"
            ),
        )

    multiplier = STRUCTURE_MULTIPLIERS[structure_type]

    base_score = 30.0
    if year_built < 2000:
        base_score += 25.0

    # Wind exposure contributes proportionally to how close the storm is
    # to a "major hurricane" benchmark (130 mph).
    wind_exposure_score = min(30.0, (storm_wind_mph / 130.0) * 30.0)
    base_score += wind_exposure_score

    score = base_score * multiplier

    non_resistant = (
        county["bcat_wind_resistance"] == "Not Resistant"
        or county["bcat_flood_resistance"] == "Not Resistant"
    )
    if non_resistant:
        score *= 1.20  # additional damage modifier for non-resistant BCAT counties

    vulnerability_score = int(round(min(100.0, max(0.0, score))))

    base_value = STRUCTURE_BASE_VALUE_USD.get(structure_type, 250_000)
    predicted_damage_usd = round(base_value * (vulnerability_score / 100.0), 2)

    return {
        "vulnerability_score": vulnerability_score,
        "predicted_damage_usd": predicted_damage_usd,
    }


def build_recommendation(
    vulnerability_score: int, county: Dict[str, Any], shelters_found: int
) -> str:
    if vulnerability_score >= 75:
        risk_level = "CRITICAL"
        action = "Evacuate immediately to a rated shelter before storm onset."
    elif vulnerability_score >= 50:
        risk_level = "HIGH"
        action = "Strongly consider relocating to a rated shelter as conditions develop."
    elif vulnerability_score >= 25:
        risk_level = "MODERATE"
        action = "Shelter-in-place may be viable if the structure meets code; monitor updates."
    else:
        risk_level = "LOW"
        action = "Structure is likely resilient for this event; monitor local alerts."

    code_note = (
        f" {county['county_name']} building codes are rated '{county['bcat_wind_resistance']}' "
        f"for wind, under the {county['building_code_era']} standard."
    )

    if shelters_found == 0:
        shelter_note = " WARNING: No known shelters in the database can survive this storm's peak winds."
    else:
        shelter_note = f" {shelters_found} survivable shelter(s) identified nearby."

    return f"[{risk_level}] {action}{code_note}{shelter_note}"


def find_survivable_shelters(
    lat: float, lon: float, storm_wind_mph: float, limit: int = 3
) -> List[ShelterResult]:
    survivable = [s for s in SHELTERS if s["max_wind_rating_mph"] >= storm_wind_mph]

    ranked = []
    for shelter in survivable:
        distance = haversine_miles(lat, lon, shelter["lat"], shelter["lon"])
        nav_url = (
            "https://www.google.com/maps/dir/?api=1&destination="
            f"{shelter['lat']},{shelter['lon']}"
        )
        ranked.append(
            ShelterResult(
                name=shelter["name"],
                county=shelter["county"],
                max_wind_rating_mph=shelter["max_wind_rating_mph"],
                capacity=shelter["capacity"],
                pet_friendly=shelter["pet_friendly"],
                ada_accessible=shelter["ada_accessible"],
                generator=shelter["generator"],
                distance_miles=round(distance, 2),
                lat=shelter["lat"],
                lon=shelter["lon"],
                navigation_url=nav_url,
            )
        )

    ranked.sort(key=lambda s: s.distance_miles)
    return ranked[:limit]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/shelters")
def get_shelters() -> List[Dict[str, Any]]:
    return SHELTERS


@app.post("/api/evaluate", response_model=EvaluateResponse)
def evaluate(payload: EvaluateRequest) -> EvaluateResponse:
    county = match_county(payload.lat, payload.lon)

    if payload.scenario_wind_mph is not None:
        storm_wind_mph = float(payload.scenario_wind_mph)
    else:
        storm_wind_mph = fetch_peak_wind_gust(payload.lat, payload.lon)

    vuln = calculate_vulnerability(
        payload.year_built, payload.structure_type, county, storm_wind_mph
    )

    survivable_shelters = find_survivable_shelters(payload.lat, payload.lon, storm_wind_mph)

    recommendation = build_recommendation(
        vuln["vulnerability_score"], county, len(survivable_shelters)
    )

    return EvaluateResponse(
        county_name=county["county_name"],
        county_fips=county["county_fips"],
        bcat_wind_resistance=county["bcat_wind_resistance"],
        bcat_flood_resistance=county["bcat_flood_resistance"],
        building_code_era=county["building_code_era"],
        storm_wind_mph=storm_wind_mph,
        vulnerability_score=vuln["vulnerability_score"],
        predicted_damage_usd=vuln["predicted_damage_usd"],
        recommendation=recommendation,
        survivable_shelters=survivable_shelters,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
