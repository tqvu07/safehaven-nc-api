"""
SafeHaven NC — Emergency Resilience and Adaptive Shelter Router
Carolina Data Challenge

Single-file async FastAPI service providing:
  - Dynamic county + FIPS resolution via US Census TIGERweb REST API
  - Dynamic FEMA BCAT building-code lookup via FEMA GeoPlatform REST API
  - Live FEMA National Shelter System (NSS) ingestion with nc_shelters.json fallback
  - HAZUS-style structural vulnerability / predicted-damage scoring
  - Survivable-shelter filtering and routing (Haversine + Google Maps links)
  - Live multi-metric weather via Open-Meteo
"""

import asyncio
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SafeHaven NC",
    description="Emergency resilience and adaptive shelter router for the NC Triangle region.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> Dict[str, str]:
    return {"service": "SafeHaven NC API", "status": "online", "docs": "/docs"}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Hard 5-second wall-clock limit on every outbound request.
REQUEST_TIMEOUT = 5.0

# US Census TIGERweb — county spatial query (State_County layer 1).
CENSUS_COUNTY_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/State_County/MapServer/1/query"
)

# FEMA GeoPlatform — Building Code Adoption Tracking (BCAT).
FEMA_BCAT_URL = (
    "https://gis.fema.gov/arcgis/rest/services/FEMA/BCAT_County/MapServer/0/query"
)

# FEMA National Shelter System — open / active shelters (ArcGIS FeatureServer).
NSS_SHELTERS_URL = (
    "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/"
    "National_Shelter_System_Open_Shelters/FeatureServer/0/query"
)

# Open-Meteo free forecast API.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Companion fallback file — used when NSS returns 0 active NC shelters.
# See nc_shelters.json for the expected array-of-objects schema.
NC_SHELTERS_FALLBACK_PATH = Path(__file__).parent / "nc_shelters.json"

# HAZUS structure vulnerability multipliers (damage and score).
STRUCTURE_MULTIPLIERS: Dict[str, float] = {
    "mobile_home": 2.2,
    "single_family": 1.0,
    "multi_family": 0.7,
}

# Baseline replacement values used in the piecewise damage curve.
STRUCTURE_BASE_VALUE_USD: Dict[str, int] = {
    "mobile_home": 80_000,
    "single_family": 300_000,
    "multi_family": 550_000,
}

# NSS data carries no structural wind rating; apply IBC Risk Category III
# minimum (~110 mph design speed) as a conservative default for all live
# NSS shelters that lack an explicit field.
NSS_DEFAULT_WIND_RATING_MPH: float = 110.0

# Cache TTLs (seconds).
TTL_COUNTY = 3_600       # 1 hour  — county boundaries are stable
TTL_BCAT = 86_400        # 24 hours — code adoption data changes rarely
TTL_SHELTERS = 900       # 15 minutes — live shelter populations update often


# ---------------------------------------------------------------------------
# Minimal in-memory TTL cache
# ---------------------------------------------------------------------------

class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl_seconds: float) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl_seconds


_CACHE: Dict[str, _CacheEntry] = {}


def cache_get(key: str) -> Optional[Any]:
    entry = _CACHE.get(key)
    if entry and time.monotonic() < entry.expires_at:
        return entry.value
    _CACHE.pop(key, None)
    return None


def cache_set(key: str, value: Any, ttl_seconds: float) -> None:
    _CACHE[key] = _CacheEntry(value, ttl_seconds)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class EvaluateRequest(BaseModel):
    lat: float
    lon: float
    year_built: int
    structure_type: str = Field(..., description="mobile_home | single_family | multi_family")
    scenario_wind_mph: Optional[float] = None


class WeatherInfo(BaseModel):
    current_temp_f: float
    wind_gusts_mph: float
    sustained_wind_mph: float
    precipitation_next_24h_in: float


class ShelterResult(BaseModel):
    name: str
    county: str
    max_wind_rating_mph: float
    evacuation_capacity: int
    remaining_capacity: int
    total_population: int
    shelter_status: str
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
    weather: WeatherInfo
    survivable_shelters: List[ShelterResult]


# ---------------------------------------------------------------------------
# Shared utility helpers
# ---------------------------------------------------------------------------

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in miles."""
    r = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_bool_field(
    value: Any,
    truthy: tuple = ("yes", "1", "true", "y"),
) -> bool:
    """Normalise string/int/bool field values to Python bool."""
    return str(value).strip().lower() in truthy


def _resistant_label(raw: Any) -> str:
    """Map a raw BCAT field value to 'Resistant' or 'Not Resistant'."""
    return (
        "Resistant"
        if _parse_bool_field(raw, ("yes", "1", "true", "resistant", "y"))
        else "Not Resistant"
    )


# ---------------------------------------------------------------------------
# External API clients
# ---------------------------------------------------------------------------

async def resolve_county(lat: float, lon: float) -> Dict[str, str]:
    """
    Resolve county name and 5-digit FIPS from coordinates via the Census
    TIGERweb State_County MapServer (layer 1).

    Returns a dict with keys:
        county_name       – display name, e.g. "Orange County"
        county_name_clean – bare name for API queries, e.g. "Orange"
        county_fips       – 5-digit string, e.g. "37135"

    Cache TTL: 1 hour (keyed to lat/lon rounded to 2 decimal places ≈ 1 km).
    """
    cache_key = f"county_{round(lat, 2)}_{round(lon, 2)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "NAME,GEOID",
        "returnGeometry": "false",
        "f": "json",
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(CENSUS_COUNTY_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except httpx.TimeoutException:
            raise HTTPException(502, "Census TIGERweb API timed out (5 s).")
        except httpx.HTTPError as exc:
            raise HTTPException(502, f"Census TIGERweb API error: {exc}")

    features = data.get("features", [])
    if not features:
        raise HTTPException(
            404,
            f"No US county found for coordinates ({lat}, {lon}). "
            "Verify the point falls within a recognised US county boundary.",
        )

    attrs = features[0]["attributes"]
    bare_name: str = str(attrs.get("NAME", "Unknown")).strip()
    fips: str = str(attrs.get("GEOID", "00000")).strip()

    # TIGERweb NAME is the bare county name without the "County" suffix.
    display_name = (
        bare_name if "county" in bare_name.lower() else f"{bare_name} County"
    )

    result: Dict[str, str] = {
        "county_name": display_name,
        "county_name_clean": bare_name,
        "county_fips": fips,
    }
    cache_set(cache_key, result, ttl_seconds=TTL_COUNTY)
    return result


async def fetch_bcat(fips: str, county_name_clean: str) -> Dict[str, str]:
    """
    Fetch FEMA BCAT building-code attributes (wind resistance, flood
    resistance, code edition) for a county identified by FIPS or name.

    Cache TTL: 24 hours. Falls back gracefully to conservative defaults
    on timeout or if the county record is absent from the dataset.
    """
    cache_key = f"bcat_{fips}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    _fallback: Dict[str, str] = {
        "bcat_wind_resistance": "Not Resistant",
        "bcat_flood_resistance": "Not Resistant",
        "building_code_era": "Legacy NC Code",
    }

    params = {
        "where": (
            f"COUNTY_FIPS = '{fips}' OR "
            f"(COUNTY = '{county_name_clean}' AND STATE = 'NC')"
        ),
        "outFields": "COUNTY,WIND_RESISTANT,FLOOD_RESISTANT,CODE_EDITION,BCAT_STATUS",
        "returnGeometry": "false",
        "f": "json",
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(FEMA_BCAT_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.TimeoutException, httpx.HTTPError):
            # Do not crash — return conservative defaults and cache them.
            cache_set(cache_key, _fallback, ttl_seconds=TTL_BCAT)
            return _fallback

    features = data.get("features", [])
    if not features:
        cache_set(cache_key, _fallback, ttl_seconds=TTL_BCAT)
        return _fallback

    attrs = features[0]["attributes"]
    result: Dict[str, str] = {
        "bcat_wind_resistance": _resistant_label(attrs.get("WIND_RESISTANT", "no")),
        "bcat_flood_resistance": _resistant_label(attrs.get("FLOOD_RESISTANT", "no")),
        "building_code_era": str(attrs.get("CODE_EDITION") or "Standard NC Code").strip(),
    }
    cache_set(cache_key, result, ttl_seconds=TTL_BCAT)
    return result


async def fetch_weather(lat: float, lon: float) -> WeatherInfo:
    """
    Fetch live multi-metric weather from Open-Meteo:
      - current_temp_f           Current 2 m air temperature (°F)
      - sustained_wind_mph       Current 10 m wind speed (mph)
      - wind_gusts_mph           Peak 10 m gust over the next 24 hours (mph)
      - precipitation_next_24h_in  Accumulated precipitation over 24 h (inches)
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_gusts_10m,windspeed_10m,precipitation",
        "current_weather": "true",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "forecast_days": 1,
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(OPEN_METEO_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            raise HTTPException(502, f"Open-Meteo API unavailable: {exc}")

    current = data.get("current_weather", {})
    hourly = data.get("hourly", {})

    current_temp_f = float(current.get("temperature", 0.0))
    sustained_mph = float(current.get("windspeed", 0.0))

    gusts: List[float] = [
        float(g) for g in hourly.get("wind_gusts_10m", []) if g is not None
    ]
    precip: List[float] = [
        float(p) for p in hourly.get("precipitation", []) if p is not None
    ]

    peak_gust_mph = max(gusts) if gusts else sustained_mph
    total_precip_in = round(sum(precip), 3)

    return WeatherInfo(
        current_temp_f=round(current_temp_f, 1),
        wind_gusts_mph=round(peak_gust_mph, 1),
        sustained_wind_mph=round(sustained_mph, 1),
        precipitation_next_24h_in=total_precip_in,
    )


def _load_nc_shelters_fallback() -> List[Dict[str, Any]]:
    """
    Load the designated-facility inventory from nc_shelters.json.

    Expected JSON schema — array of objects with these fields:
      name                str   Facility display name
      county              str   County name (bare, e.g. "Orange")
      lat                 float WGS-84 latitude
      lon                 float WGS-84 longitude
      max_wind_rating_mph float Structural wind survival limit
      evacuation_capacity int   Total rated occupancy
      total_population    int   Current occupant count (0 = unoccupied)
      shelter_status      str   e.g. "DESIGNATED" | "OPEN" | "CLOSED"
      pet_friendly        bool
      ada_accessible      bool
      generator           bool

    Returns an empty list on any read / parse error so the endpoint can
    still respond (with an empty shelter list) rather than crash.
    """
    if not NC_SHELTERS_FALLBACK_PATH.exists():
        return []
    try:
        return json.loads(NC_SHELTERS_FALLBACK_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _normalise_nss_feature(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert a single GeoJSON Feature from the NSS FeatureServer into the
    internal shelter dict schema.  Returns None for features with no geometry.
    """
    geom = feat.get("geometry") or {}
    coords = geom.get("coordinates", [])
    if len(coords) < 2 or coords[0] is None or coords[1] is None:
        return None

    lon, lat = float(coords[0]), float(coords[1])
    props = feat.get("properties") or {}

    evac_cap = int(props.get("EVACUATION_CAPACITY") or 300)
    total_pop = int(props.get("TOTAL_POPULATION") or 0)

    return {
        "name": str(props.get("SHELTER_NAME") or "Unnamed Shelter").strip(),
        "county": str(props.get("COUNTY") or "Unknown").strip(),
        "lat": lat,
        "lon": lon,
        "max_wind_rating_mph": NSS_DEFAULT_WIND_RATING_MPH,
        "evacuation_capacity": evac_cap,
        "total_population": total_pop,
        "remaining_capacity": max(0, evac_cap - total_pop),
        "shelter_status": str(props.get("SHELTER_STATUS") or "OPEN").upper().strip(),
        "pet_friendly": _parse_bool_field(props.get("PET_FRIENDLY", "no")),
        "ada_accessible": _parse_bool_field(props.get("ADA_COMPLIANT", "no")),
        "generator": _parse_bool_field(props.get("GENERATOR_ON_SITE", "no")),
    }


def _normalise_fallback_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalise a single nc_shelters.json object into the internal shelter dict
    schema, filling in any missing fields with safe defaults.
    """
    evac_cap = int(raw.get("evacuation_capacity", raw.get("capacity", 300)))
    total_pop = int(raw.get("total_population", 0))
    return {
        "name": str(raw.get("name", "Unnamed")).strip(),
        "county": str(raw.get("county", "Unknown")).strip(),
        "lat": float(raw["lat"]),
        "lon": float(raw["lon"]),
        "max_wind_rating_mph": float(
            raw.get("max_wind_rating_mph", NSS_DEFAULT_WIND_RATING_MPH)
        ),
        "evacuation_capacity": evac_cap,
        "total_population": total_pop,
        "remaining_capacity": max(0, evac_cap - total_pop),
        "shelter_status": str(raw.get("shelter_status", "DESIGNATED")).upper().strip(),
        "pet_friendly": bool(raw.get("pet_friendly", False)),
        "ada_accessible": bool(raw.get("ada_accessible", False)),
        "generator": bool(raw.get("generator", False)),
    }


async def fetch_shelters() -> List[Dict[str, Any]]:
    """
    Fetch open NC emergency shelters from the FEMA NSS ArcGIS FeatureServer.

    Cache TTL: 15 minutes.

    Fallback: if the live feed returns 0 records (no active NC disaster
    declaration), load the designated-facility inventory from nc_shelters.json.
    """
    cache_key = "nss_shelters_nc"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    params = {
        "where": "STATE = 'NC'",
        "outFields": (
            "SHELTER_NAME,CITY,COUNTY,EVACUATION_CAPACITY,POST_IMPACT_CAPACITY,"
            "TOTAL_POPULATION,SHELTER_STATUS,PET_FRIENDLY,ADA_COMPLIANT,GENERATOR_ON_SITE"
        ),
        "f": "geojson",
    }

    shelters: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(NSS_SHELTERS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            features: List[Dict[str, Any]] = data.get("features", [])
        except (httpx.TimeoutException, httpx.HTTPError):
            features = []

    for feat in features:
        normalised = _normalise_nss_feature(feat)
        if normalised is not None:
            shelters.append(normalised)

    if not shelters:
        # No active disaster shelters open — use designated-facility fallback.
        for raw in _load_nc_shelters_fallback():
            try:
                shelters.append(_normalise_fallback_record(raw))
            except (KeyError, ValueError, TypeError):
                # Skip malformed records rather than crashing.
                continue

    cache_set(cache_key, shelters, ttl_seconds=TTL_SHELTERS)
    return shelters


# ---------------------------------------------------------------------------
# Vulnerability & recommendation logic (HAZUS-style piecewise curve)
# ---------------------------------------------------------------------------

def calculate_vulnerability(
    year_built: int,
    structure_type: str,
    bcat: Dict[str, str],
    storm_wind_mph: float,
) -> Dict[str, Any]:
    """
    HAZUS-style piecewise wind-damage curve anchored to Saffir-Simpson
    thresholds.  Returns vulnerability_score (0–100) and predicted_damage_usd.

    Wind-anchored score bands (hard-bounded to prevent bleed-over):
        < 40 mph  →  LOW        score  0–19   $0 structural damage
        40–73 mph →  MODERATE   score 20–49   1–5 % of base value
        74–95 mph →  HIGH       score 50–74   5–20 %
        96+ mph   →  EVACUATE   score 75–100  20–100 %
    """
    if structure_type not in STRUCTURE_MULTIPLIERS:
        raise HTTPException(
            400,
            f"Invalid structure_type '{structure_type}'. "
            f"Must be one of: {list(STRUCTURE_MULTIPLIERS.keys())}",
        )

    multiplier = STRUCTURE_MULTIPLIERS[structure_type]
    base_value = STRUCTURE_BASE_VALUE_USD.get(structure_type, 250_000)
    non_resistant = (
        bcat["bcat_wind_resistance"] == "Not Resistant"
        or bcat["bcat_flood_resistance"] == "Not Resistant"
    )
    bcat_mod = 1.20 if non_resistant else 1.0

    # ── Damage fraction (piecewise linear per Saffir-Simpson band) ──────────
    if storm_wind_mph < 40.0:
        # Sub-tropical — cosmetic debris only, zero structural damage.
        damage_fraction = 0.0
    elif storm_wind_mph < 74.0:
        # Tropical storm (40–73 mph): roof shingles / siding loss, 1–5 %.
        t = (storm_wind_mph - 40.0) / 34.0
        damage_fraction = 0.01 + 0.04 * t
    elif storm_wind_mph < 96.0:
        # Hurricane Cat 1 (74–95 mph): moderate structural damage, 5–20 %.
        t = (storm_wind_mph - 74.0) / 22.0
        damage_fraction = 0.05 + 0.15 * t
    elif storm_wind_mph < 111.0:
        # Hurricane Cat 2 (96–110 mph): extensive damage, 20–40 %.
        t = (storm_wind_mph - 96.0) / 15.0
        damage_fraction = 0.20 + 0.20 * t
    elif storm_wind_mph < 130.0:
        # Hurricane Cat 3 (111–129 mph): devastating damage, 40–80 %.
        t = (storm_wind_mph - 111.0) / 19.0
        damage_fraction = 0.40 + 0.40 * t
    else:
        # Hurricane Cat 4+ (≥ 130 mph): catastrophic / near-total loss.
        t = min(1.0, (storm_wind_mph - 130.0) / 30.0)
        damage_fraction = 0.80 + 0.20 * t

    adjusted_fraction = min(1.0, damage_fraction * multiplier * bcat_mod)
    predicted_damage_usd = round(base_value * adjusted_fraction, 2)

    # ── Vulnerability score (0–100, wind-band anchored) ─────────────────────
    if storm_wind_mph < 40.0:
        # LOW band: hard ceiling at 19.
        # BCAT modifier excluded — light wind does not load the structure.
        raw = (storm_wind_mph / 40.0) * 12.0
        if year_built < 2000:
            raw += 2.0
        vulnerability_score = int(round(min(19.0, max(0.0, raw))))

    elif storm_wind_mph < 74.0:
        # MODERATE band: clamped 20–49.
        t = (storm_wind_mph - 40.0) / 34.0
        raw = 20.0 + 20.0 * t
        raw += (multiplier - 1.0) * 8.0   # +9.6 mobile home, −2.4 multi-family
        if year_built < 2000:
            raw += 4.0
        if non_resistant:
            raw += 3.0
        vulnerability_score = int(round(min(49.0, max(20.0, raw))))

    else:
        # HIGH / EVACUATE band: clamped 50–100.
        t = min(1.0, (storm_wind_mph - 74.0) / 56.0)
        raw = 50.0 + 35.0 * t
        raw += (multiplier - 1.0) * 12.0  # +14.4 mobile home, −3.6 multi-family
        if year_built < 2000:
            raw += 6.0
        if non_resistant:
            raw += 5.0
        vulnerability_score = int(round(min(100.0, max(50.0, raw))))

    return {
        "vulnerability_score": vulnerability_score,
        "predicted_damage_usd": predicted_damage_usd,
    }


def build_recommendation(
    vulnerability_score: int,
    county_name: str,
    bcat: Dict[str, str],
    shelters_found: int,
) -> str:
    """
    Map vulnerability_score to a risk label and plain-language action.

    Score → band alignment:
        0–19  → [LOW RISK]
       20–49  → [MODERATE RISK]
       50–74  → [HIGH RISK]
       75–100 → [EVACUATE]
    """
    if vulnerability_score >= 75:
        risk_level = "EVACUATE"
        action = (
            "Evacuate immediately to a rated emergency shelter before storm onset. "
            "This structure type is at severe risk of catastrophic failure under "
            "current forecast wind speeds."
        )
    elif vulnerability_score >= 50:
        risk_level = "HIGH RISK"
        action = (
            "Significant structural threat detected. Strongly consider relocating "
            "to a rated shelter now as conditions develop."
        )
    elif vulnerability_score >= 20:
        risk_level = "MODERATE RISK"
        action = (
            "Elevated wind hazard. Shelter-in-place may be viable for fully "
            "code-compliant structures; monitor NWS forecasts closely and "
            "prepare to evacuate if conditions intensify."
        )
    else:
        # LOW RISK — surface BCAT context as the primary actionable note.
        return (
            f"[LOW RISK] Current wind conditions pose minimal structural threat; "
            "no evacuation action is required at this time. "
            f"Note: {county_name} carries a '{bcat['bcat_wind_resistance']}' FEMA BCAT "
            f"wind-resistance rating under the {bcat['building_code_era']} standard — "
            "verify your structure meets current code requirements before any "
            "future severe weather season."
        )

    code_note = (
        f" {county_name} building codes are rated '{bcat['bcat_wind_resistance']}' "
        f"for wind under the {bcat['building_code_era']} standard."
    )
    shelter_note = (
        " WARNING: No known shelters can survive this storm's peak winds."
        if shelters_found == 0
        else f" {shelters_found} survivable shelter(s) identified nearby."
    )
    return f"[{risk_level}] {action}{code_note}{shelter_note}"


def find_survivable_shelters(
    lat: float,
    lon: float,
    storm_wind_mph: float,
    all_shelters: List[Dict[str, Any]],
    limit: int = 3,
) -> List[ShelterResult]:
    """
    Filter shelters whose structural wind rating meets or exceeds the storm
    speed, rank by distance, and return the nearest `limit` results.
    """
    results: List[ShelterResult] = []

    for s in all_shelters:
        if s["max_wind_rating_mph"] < storm_wind_mph:
            continue
        dist = haversine_miles(lat, lon, s["lat"], s["lon"])
        results.append(
            ShelterResult(
                name=s["name"],
                county=s["county"],
                max_wind_rating_mph=s["max_wind_rating_mph"],
                evacuation_capacity=s["evacuation_capacity"],
                remaining_capacity=s["remaining_capacity"],
                total_population=s["total_population"],
                shelter_status=s["shelter_status"],
                pet_friendly=s["pet_friendly"],
                ada_accessible=s["ada_accessible"],
                generator=s["generator"],
                distance_miles=round(dist, 2),
                lat=s["lat"],
                lon=s["lon"],
                navigation_url=(
                    "https://www.google.com/maps/dir/?api=1&destination="
                    f"{s['lat']},{s['lon']}"
                ),
            )
        )

    results.sort(key=lambda r: r.distance_miles)
    return results[:limit]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/shelters")
async def get_shelters() -> List[Dict[str, Any]]:
    """Return the current shelter inventory (live NSS or fallback)."""
    return await fetch_shelters()


@app.post("/api/evaluate", response_model=EvaluateResponse)
async def evaluate(payload: EvaluateRequest) -> EvaluateResponse:
    """
    Full evaluation pipeline:

    Step 1  resolve_county + fetch_weather + fetch_shelters  — parallel
    Step 2  fetch_bcat                                       — after county
    Step 3  calculate_vulnerability
    Step 4  find_survivable_shelters
    Step 5  build_recommendation
    """
    # ── Step 1: fire independent fetches concurrently ───────────────────────
    # county and (weather + shelters) run in parallel; weather and shelters
    # also run in parallel with each other inside the nested gather.
    county, (weather, all_shelters) = await asyncio.gather(
        resolve_county(payload.lat, payload.lon),
        asyncio.gather(
            fetch_weather(payload.lat, payload.lon),
            fetch_shelters(),
        ),
    )

    # ── Step 2: BCAT requires county FIPS — runs after step 1 ───────────────
    bcat = await fetch_bcat(county["county_fips"], county["county_name_clean"])

    # ── Step 3: determine storm wind speed ──────────────────────────────────
    # scenario_wind_mph overrides live forecast (useful for what-if analysis).
    storm_wind_mph = (
        float(payload.scenario_wind_mph)
        if payload.scenario_wind_mph is not None
        else weather.wind_gusts_mph
    )

    # ── Step 4: score vulnerability ─────────────────────────────────────────
    vuln = calculate_vulnerability(
        payload.year_built, payload.structure_type, bcat, storm_wind_mph
    )

    # ── Step 5: filter and rank shelters ────────────────────────────────────
    survivable = find_survivable_shelters(
        payload.lat, payload.lon, storm_wind_mph, all_shelters
    )

    # ── Step 6: compose recommendation ─────────────────────────────────────
    recommendation = build_recommendation(
        vuln["vulnerability_score"],
        county["county_name"],
        bcat,
        len(survivable),
    )

    return EvaluateResponse(
        county_name=county["county_name"],
        county_fips=county["county_fips"],
        bcat_wind_resistance=bcat["bcat_wind_resistance"],
        bcat_flood_resistance=bcat["bcat_flood_resistance"],
        building_code_era=bcat["building_code_era"],
        storm_wind_mph=storm_wind_mph,
        vulnerability_score=vuln["vulnerability_score"],
        predicted_damage_usd=vuln["predicted_damage_usd"],
        recommendation=recommendation,
        weather=weather,
        survivable_shelters=survivable,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
