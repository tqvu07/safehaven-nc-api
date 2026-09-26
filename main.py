"""
SafeHaven NC — Emergency Resilience and Adaptive Shelter Router
Version 3.0.0 — Full 100-county NC coverage
Carolina Data Challenge

Architecture
────────────
Startup  : Bulk FEMA BCAT ingestion for all 100 NC counties → STATEWIDE_BCAT dict
Runtime  : Census TIGERweb spatial geocoding → O(1) STATEWIDE_BCAT lookup (no per-request BCAT call)
Parallel : weather + NSS shelters fire concurrently with county geocoding
Routing  : Statewide Haversine ranking across every NC shelter record
Lineage  : data_sources block in every EvaluateResponse
"""

import asyncio
import json
import math
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
NC_BCAT_FALLBACK_PATH = _HERE / "nc_bcat_statewide.json"
NC_SHELTERS_FALLBACK_PATH = _HERE / "nc_shelters.json"


# ---------------------------------------------------------------------------
# External API endpoints
# ---------------------------------------------------------------------------

CENSUS_COUNTY_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/State_County/MapServer/1/query"
)
FEMA_BCAT_URL = (
    "https://gis.fema.gov/arcgis/rest/services/FEMA/BCAT_County/MapServer/0/query"
)
NSS_SHELTERS_URL = (
    "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/"
    "National_Shelter_System_Open_Shelters/FeatureServer/0/query"
)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


# ---------------------------------------------------------------------------
# Tunable constants
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT: float = 5.0

# Shelter routing
NSS_DEFAULT_WIND_RATING_MPH: float = 110.0  # IBC Risk Category III minimum
MAX_SHELTERS_RETURNED: int = 5

# HAZUS vulnerability
STRUCTURE_MULTIPLIERS: Dict[str, float] = {
    "mobile_home": 2.2,
    "single_family": 1.0,
    "multi_family": 0.7,
}
STRUCTURE_BASE_VALUE_USD: Dict[str, int] = {
    "mobile_home": 80_000,
    "single_family": 300_000,
    "multi_family": 550_000,
}

# Cache TTLs (seconds)
TTL_COUNTY: float = 3_600.0    # 1 hour  — county boundaries are stable
TTL_SHELTERS: float = 900.0    # 15 min  — NSS populations update frequently

# Conservative defaults applied when a county FIPS is absent from STATEWIDE_BCAT.
# "Not Resistant" is intentionally conservative for emergency management purposes.
_DEFAULT_BCAT: Dict[str, str] = {
    "bcat_wind_resistance": "Not Resistant",
    "bcat_flood_resistance": "Not Resistant",
    "building_code_era": "Standard NC Code",
}


# ---------------------------------------------------------------------------
# Module-level state — populated during startup, never mutated at runtime
# ---------------------------------------------------------------------------

# Maps 5-digit FIPS → {bcat_wind_resistance, bcat_flood_resistance, building_code_era}
# for every NC county. Populated by ingest_statewide_bcat() at process start.
STATEWIDE_BCAT: Dict[str, Dict[str, str]] = {}
BCAT_SOURCE: str = "FEMA_BCAT_STATEWIDE_FALLBACK"


# ---------------------------------------------------------------------------
# Minimal in-memory TTL cache
# ---------------------------------------------------------------------------

class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl: float) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl


_CACHE: Dict[str, _CacheEntry] = {}


def cache_get(key: str) -> Optional[Any]:
    entry = _CACHE.get(key)
    if entry and time.monotonic() < entry.expires_at:
        return entry.value
    _CACHE.pop(key, None)
    return None


def cache_set(key: str, value: Any, ttl: float) -> None:
    _CACHE[key] = _CacheEntry(value, ttl)


# ---------------------------------------------------------------------------
# Shared utility helpers
# ---------------------------------------------------------------------------

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS-84 points, in miles."""
    r = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_bool_field(value: Any, truthy: tuple = ("yes", "1", "true", "y")) -> bool:
    return str(value).strip().lower() in truthy


def _resistant_label(raw: Any) -> str:
    """Map a raw BCAT field value to 'Resistant' or 'Not Resistant'."""
    return (
        "Resistant"
        if _parse_bool_field(raw, ("yes", "1", "true", "resistant", "y"))
        else "Not Resistant"
    )


# ---------------------------------------------------------------------------
# Startup: bulk FEMA BCAT ingestion for all 100 NC counties
# ---------------------------------------------------------------------------

async def ingest_statewide_bcat() -> None:
    """
    Fetch all NC county BCAT records from FEMA GeoPlatform and index them in
    STATEWIDE_BCAT by 5-digit FIPS code.

    Priority order:
      1. FEMA live service (STATE = 'NC' query, all outFields)
      2. nc_bcat_statewide.json local fallback
      3. Empty dict — per-county lookups fall back to _DEFAULT_BCAT

    Called once at process startup via the lifespan context manager.
    """
    global STATEWIDE_BCAT, BCAT_SOURCE

    params = {
        "where": "STATE = 'NC'",
        "outFields": (
            "COUNTY,COUNTY_FIPS,WIND_RESISTANT,FLOOD_RESISTANT,"
            "CODE_EDITION,BCAT_STATUS"
        ),
        "returnGeometry": "false",
        "f": "json",
    }

    live_features: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(FEMA_BCAT_URL, params=params)
            resp.raise_for_status()
            live_features = resp.json().get("features", [])
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            print(
                f"[SafeHaven startup] FEMA BCAT live fetch failed ({exc}); "
                "falling back to nc_bcat_statewide.json."
            )

    if live_features:
        bcat_map: Dict[str, Dict[str, str]] = {}
        for feat in live_features:
            attrs = feat.get("attributes", {})
            fips = str(attrs.get("COUNTY_FIPS") or "").strip().zfill(5)
            if len(fips) != 5 or fips == "00000":
                continue
            bcat_map[fips] = {
                "bcat_wind_resistance": _resistant_label(
                    attrs.get("WIND_RESISTANT", "no")
                ),
                "bcat_flood_resistance": _resistant_label(
                    attrs.get("FLOOD_RESISTANT", "no")
                ),
                "building_code_era": str(
                    attrs.get("CODE_EDITION") or "Standard NC Code"
                ).strip(),
            }
        STATEWIDE_BCAT = bcat_map
        BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_LIVE"
        print(
            f"[SafeHaven startup] BCAT ingestion complete — "
            f"{len(STATEWIDE_BCAT)} NC counties loaded from FEMA live service."
        )
        return

    # ── Fallback: nc_bcat_statewide.json ────────────────────────────────────
    if NC_BCAT_FALLBACK_PATH.exists():
        try:
            records: List[Dict[str, Any]] = json.loads(
                NC_BCAT_FALLBACK_PATH.read_text(encoding="utf-8")
            )
            bcat_map = {}
            for rec in records:
                fips = str(rec.get("county_fips") or "").strip().zfill(5)
                if len(fips) != 5 or fips == "00000":
                    continue
                bcat_map[fips] = {
                    "bcat_wind_resistance": str(
                        rec.get("bcat_wind_resistance", "Not Resistant")
                    ),
                    "bcat_flood_resistance": str(
                        rec.get("bcat_flood_resistance", "Not Resistant")
                    ),
                    "building_code_era": str(
                        rec.get("building_code_era", "Standard NC Code")
                    ),
                }
            STATEWIDE_BCAT = bcat_map
            BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_FALLBACK"
            print(
                f"[SafeHaven startup] BCAT loaded from local fallback — "
                f"{len(STATEWIDE_BCAT)} counties."
            )
        except (json.JSONDecodeError, OSError) as exc:
            print(
                f"[SafeHaven startup] WARNING: nc_bcat_statewide.json unreadable "
                f"({exc}). All counties will use conservative defaults."
            )
    else:
        print(
            "[SafeHaven startup] WARNING: nc_bcat_statewide.json not found. "
            "All counties will use conservative defaults."
        )


# ---------------------------------------------------------------------------
# Lifespan context manager
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI):  # noqa: RUF029
    await ingest_statewide_bcat()
    yield
    # Graceful shutdown hooks can be added here (e.g., close DB connections).


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SafeHaven NC",
    description=(
        "Emergency resilience and adaptive shelter router — full 100-county "
        "NC statewide coverage via live FEMA BCAT + Census TIGERweb APIs."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "service": "SafeHaven NC API",
        "version": "3.0.0",
        "coverage": "100 NC counties",
        "status": "online",
        "bcat_source": BCAT_SOURCE,
        "bcat_counties_loaded": len(STATEWIDE_BCAT),
        "docs": "/docs",
    }


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class EvaluateRequest(BaseModel):
    lat: float
    lon: float
    year_built: int
    structure_type: str = Field(
        ..., description="mobile_home | single_family | multi_family"
    )
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


class DataSources(BaseModel):
    county_resolution: str
    # "US_CENSUS_TIGERWEB_LIVE"
    bcat_rating: str
    # "FEMA_BCAT_STATEWIDE_LIVE" | "FEMA_BCAT_STATEWIDE_FALLBACK" | "FEMA_BCAT_DEFAULT"
    shelter_source: str
    # "FEMA_NSS_LIVE" | "LOCAL_STATEWIDE_INVENTORY"


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
    data_sources: DataSources


# ---------------------------------------------------------------------------
# External API clients
# ---------------------------------------------------------------------------

async def resolve_county(
    lat: float, lon: float
) -> Tuple[Dict[str, str], str]:
    """
    Spatial geocoding via US Census TIGERweb State_County layer.
    Covers any US county — no bounding boxes, no hard-coded limits.

    Returns:
        (county_dict, source_label)
        county_dict keys: county_name, county_name_clean, county_fips

    Cache TTL: 1 hour, keyed to lat/lon rounded to 2 decimal places (~1 km).
    """
    cache_key = f"county_{round(lat, 2)}_{round(lon, 2)}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached, "US_CENSUS_TIGERWEB_LIVE"

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
    bare_name = str(attrs.get("NAME", "Unknown")).strip()
    fips = str(attrs.get("GEOID", "00000")).strip()
    display_name = (
        bare_name if "county" in bare_name.lower() else f"{bare_name} County"
    )

    result: Dict[str, str] = {
        "county_name": display_name,
        "county_name_clean": bare_name,
        "county_fips": fips,
    }
    cache_set(cache_key, result, TTL_COUNTY)
    return result, "US_CENSUS_TIGERWEB_LIVE"


async def fetch_weather(lat: float, lon: float) -> WeatherInfo:
    """
    Fetch live multi-metric weather from Open-Meteo:
      - current_temp_f             Current 2 m temperature (°F)
      - wind_gusts_mph             Peak 10 m gust over next 24 h (mph)
      - sustained_wind_mph         Current 10 m wind speed (mph)
      - precipitation_next_24h_in  Total 24 h accumulated precipitation (in)
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
    gusts = [float(g) for g in hourly.get("wind_gusts_10m", []) if g is not None]
    precip = [float(p) for p in hourly.get("precipitation", []) if p is not None]

    return WeatherInfo(
        current_temp_f=round(float(current.get("temperature", 0.0)), 1),
        wind_gusts_mph=round(
            max(gusts) if gusts else float(current.get("windspeed", 0.0)), 1
        ),
        sustained_wind_mph=round(float(current.get("windspeed", 0.0)), 1),
        precipitation_next_24h_in=round(sum(precip), 3),
    )


# ── Shelter data helpers ─────────────────────────────────────────────────────

def _load_nc_shelters_fallback() -> List[Dict[str, Any]]:
    """Load nc_shelters.json; silently returns [] on any error."""
    if not NC_SHELTERS_FALLBACK_PATH.exists():
        return []
    try:
        return json.loads(NC_SHELTERS_FALLBACK_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _normalise_nss_feature(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a GeoJSON NSS Feature to the internal shelter dict. Returns None on bad geometry."""
    geom = feat.get("geometry") or {}
    coords = geom.get("coordinates", [])
    if len(coords) < 2 or coords[0] is None or coords[1] is None:
        return None

    lon_f, lat_f = float(coords[0]), float(coords[1])
    props = feat.get("properties") or {}
    evac_cap = int(props.get("EVACUATION_CAPACITY") or 300)
    total_pop = int(props.get("TOTAL_POPULATION") or 0)

    return {
        "name": str(props.get("SHELTER_NAME") or "Unnamed Shelter").strip(),
        "county": str(props.get("COUNTY") or "Unknown").strip(),
        "lat": lat_f,
        "lon": lon_f,
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
    """Normalise a nc_shelters.json record to the internal shelter dict schema."""
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


async def fetch_shelters() -> Tuple[List[Dict[str, Any]], str]:
    """
    Fetch NC emergency shelters from the FEMA NSS ArcGIS FeatureServer.
    Returns (shelters, source_label).

    Cache TTL: 15 minutes.
    Fallback: nc_shelters.json when NSS returns 0 records (no active NC
    disaster declaration). The fallback file should contain the full
    statewide designated-facility inventory.
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
    source_label = "FEMA_NSS_LIVE"

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(NSS_SHELTERS_URL, params=params)
            resp.raise_for_status()
            for feat in resp.json().get("features", []):
                norm = _normalise_nss_feature(feat)
                if norm is not None:
                    shelters.append(norm)
        except (httpx.TimeoutException, httpx.HTTPError):
            pass  # fall through to local fallback

    if not shelters:
        source_label = "LOCAL_STATEWIDE_INVENTORY"
        for raw in _load_nc_shelters_fallback():
            try:
                shelters.append(_normalise_fallback_record(raw))
            except (KeyError, ValueError, TypeError):
                continue  # skip malformed records

    result = (shelters, source_label)
    cache_set(cache_key, result, TTL_SHELTERS)
    return result


# ---------------------------------------------------------------------------
# HAZUS-style vulnerability scoring
# ---------------------------------------------------------------------------

def calculate_vulnerability(
    year_built: int,
    structure_type: str,
    bcat: Dict[str, str],
    storm_wind_mph: float,
) -> Dict[str, Any]:
    """
    HAZUS-style piecewise wind-damage curve anchored to Saffir-Simpson thresholds.

    Score bands (hard-bounded so calm conditions never bleed into MODERATE+):
        < 40 mph  → LOW        0–19    $0 structural damage
        40–73 mph → MODERATE  20–49    1–5 % of base value
        74–95 mph → HIGH      50–74    5–20 %
        96+ mph   → EVACUATE  75–100  20–100 %
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

    # ── Piecewise damage fraction ────────────────────────────────────────────
    if storm_wind_mph < 40.0:
        damage_fraction = 0.0
    elif storm_wind_mph < 74.0:
        t = (storm_wind_mph - 40.0) / 34.0
        damage_fraction = 0.01 + 0.04 * t
    elif storm_wind_mph < 96.0:
        t = (storm_wind_mph - 74.0) / 22.0
        damage_fraction = 0.05 + 0.15 * t
    elif storm_wind_mph < 111.0:
        t = (storm_wind_mph - 96.0) / 15.0
        damage_fraction = 0.20 + 0.20 * t
    elif storm_wind_mph < 130.0:
        t = (storm_wind_mph - 111.0) / 19.0
        damage_fraction = 0.40 + 0.40 * t
    else:
        t = min(1.0, (storm_wind_mph - 130.0) / 30.0)
        damage_fraction = 0.80 + 0.20 * t

    adjusted_fraction = min(1.0, damage_fraction * multiplier * bcat_mod)
    predicted_damage_usd = round(base_value * adjusted_fraction, 2)

    # ── Vulnerability score ──────────────────────────────────────────────────
    if storm_wind_mph < 40.0:
        raw = (storm_wind_mph / 40.0) * 12.0
        if year_built < 2000:
            raw += 2.0
        vulnerability_score = int(round(min(19.0, max(0.0, raw))))

    elif storm_wind_mph < 74.0:
        t = (storm_wind_mph - 40.0) / 34.0
        raw = 20.0 + 20.0 * t
        raw += (multiplier - 1.0) * 8.0   # +9.6 mobile home, −2.4 multi-family
        if year_built < 2000:
            raw += 4.0
        if non_resistant:
            raw += 3.0
        vulnerability_score = int(round(min(49.0, max(20.0, raw))))

    else:
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
    """Map vulnerability_score to a risk label and plain-language action."""
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
            "code-compliant structures; monitor NWS forecasts and prepare to "
            "evacuate if conditions intensify."
        )
    else:
        # LOW RISK: surface BCAT context as the primary actionable note.
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


# ---------------------------------------------------------------------------
# Statewide Haversine shelter routing
# ---------------------------------------------------------------------------

def find_survivable_shelters(
    lat: float,
    lon: float,
    storm_wind_mph: float,
    all_shelters: List[Dict[str, Any]],
    limit: int = MAX_SHELTERS_RETURNED,
) -> List[ShelterResult]:
    """
    Filter shelters to those rated for >= storm_wind_mph, rank by Haversine
    distance, and return the nearest `limit` results.

    With a full statewide inventory, Haversine over every record is
    sub-millisecond — no spatial index required.
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
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "bcat_source": BCAT_SOURCE,
        "bcat_counties_loaded": len(STATEWIDE_BCAT),
        "coverage": f"{len(STATEWIDE_BCAT)}/100 NC counties",
    }


@app.get("/api/shelters")
async def get_shelters() -> List[Dict[str, Any]]:
    """Return the current shelter inventory (live NSS or fallback)."""
    shelters, _ = await fetch_shelters()
    return shelters


@app.get("/api/bcat/status")
async def bcat_status() -> Dict[str, Any]:
    """Report statewide BCAT ingestion coverage — useful for startup verification."""
    return {
        "source": BCAT_SOURCE,
        "counties_loaded": len(STATEWIDE_BCAT),
        "coverage": f"{len(STATEWIDE_BCAT)}/100 NC counties",
        "fips_indexed": sorted(STATEWIDE_BCAT.keys()),
    }


@app.get("/api/bcat/{fips}")
async def get_bcat_by_fips(fips: str) -> Dict[str, Any]:
    """
    Look up BCAT building code attributes for a given 5-digit county FIPS.
    Validates statewide ingestion for any NC county.
    """
    fips = fips.strip().zfill(5)
    if fips not in STATEWIDE_BCAT:
        raise HTTPException(404, f"FIPS '{fips}' not found in statewide BCAT index.")
    return {"fips": fips, **STATEWIDE_BCAT[fips], "source": BCAT_SOURCE}


@app.post("/api/evaluate", response_model=EvaluateResponse)
async def evaluate(payload: EvaluateRequest) -> EvaluateResponse:
    """
    Full statewide evaluation pipeline.

    Step 1 — Parallel I/O (3 concurrent calls):
              resolve_county  ║  fetch_weather  ║  fetch_shelters
    Step 2 — O(1) dict lookup: STATEWIDE_BCAT[fips]  (no extra network call)
    Step 3 — calculate_vulnerability (HAZUS piecewise curve)
    Step 4 — find_survivable_shelters (statewide Haversine, nearest 5)
    Step 5 — build_recommendation + assemble DataSources lineage
    """
    # ── Step 1: parallel I/O ─────────────────────────────────────────────────
    # resolve_county returns (dict, str); fetch_shelters returns (List, str).
    # asyncio.gather preserves order and runs all three coroutines concurrently.
    county_result, inner_results = await asyncio.gather(
        resolve_county(payload.lat, payload.lon),
        asyncio.gather(
            fetch_weather(payload.lat, payload.lon),
            fetch_shelters(),
        ),
    )
    county, county_source = county_result
    weather, shelter_data = inner_results
    all_shelters, shelter_source = shelter_data

    # ── Step 2: BCAT from startup-ingested dict ──────────────────────────────
    fips = county["county_fips"]
    bcat = STATEWIDE_BCAT.get(fips)
    if bcat is None:
        # County FIPS absent from statewide index — use conservative defaults.
        bcat = _DEFAULT_BCAT.copy()
        bcat_lineage = "FEMA_BCAT_DEFAULT"
    else:
        bcat_lineage = BCAT_SOURCE

    # ── Step 3: storm wind speed (scenario override or live gust) ───────────
    storm_wind_mph = (
        float(payload.scenario_wind_mph)
        if payload.scenario_wind_mph is not None
        else weather.wind_gusts_mph
    )

    # ── Step 4: HAZUS vulnerability scoring ─────────────────────────────────
    vuln = calculate_vulnerability(
        payload.year_built, payload.structure_type, bcat, storm_wind_mph
    )

    # ── Step 5: statewide Haversine shelter routing ──────────────────────────
    survivable = find_survivable_shelters(
        payload.lat, payload.lon, storm_wind_mph, all_shelters
    )

    recommendation = build_recommendation(
        vuln["vulnerability_score"],
        county["county_name"],
        bcat,
        len(survivable),
    )

    return EvaluateResponse(
        county_name=county["county_name"],
        county_fips=fips,
        bcat_wind_resistance=bcat["bcat_wind_resistance"],
        bcat_flood_resistance=bcat["bcat_flood_resistance"],
        building_code_era=bcat["building_code_era"],
        storm_wind_mph=storm_wind_mph,
        vulnerability_score=vuln["vulnerability_score"],
        predicted_damage_usd=vuln["predicted_damage_usd"],
        recommendation=recommendation,
        weather=weather,
        survivable_shelters=survivable,
        data_sources=DataSources(
            county_resolution=county_source,
            bcat_rating=bcat_lineage,
            shelter_source=shelter_source,
        ),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
