"""
SafeHaven NC — Emergency Resilience and Adaptive Shelter Router
Version 3.6.0 — Adaptive Concentric Ring Routing (25mi -> 50mi -> 100mi)
Carolina Data Challenge
"""

import asyncio
import json
import math
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# File paths & Endpoints
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
NC_BCAT_FALLBACK_PATH = _HERE / "nc_bcat_statewide.json"
NC_SHELTERS_FALLBACK_PATH = _HERE / "nc_shelters.json"

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

REQUEST_TIMEOUT: float = 6.0
NSS_DEFAULT_WIND_RATING_MPH: float = 120.0
MAX_SHELTERS_RETURNED: int = 5
DEFAULT_INITIAL_RADIUS_MILES: float = 25.0
MAX_EXPANDED_RADIUS_MILES: float = 100.0

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

TTL_COUNTY: float = 3_600.0
TTL_SHELTERS: float = 900.0

# ---------------------------------------------------------------------------
# Statewide 100 NC Counties Baseline
# ---------------------------------------------------------------------------

ALL_100_NC_COUNTIES: Dict[str, str] = {
    "37001": "Alamance", "37003": "Alexander", "37005": "Alleghany", "37007": "Anson",
    "37009": "Ashe", "37011": "Avery", "37013": "Beaufort", "37015": "Bertie",
    "37017": "Bladen", "37019": "Brunswick", "37021": "Buncombe", "37023": "Burke",
    "37025": "Cabarrus", "37027": "Caldwell", "37029": "Camden", "37031": "Carteret",
    "37033": "Caswell", "37035": "Catawba", "37037": "Chatham", "37039": "Cherokee",
    "37041": "Chowan", "37043": "Clay", "37045": "Cleveland", "37047": "Columbus",
    "37049": "Craven", "37051": "Cumberland", "37053": "Currituck", "37055": "Dare",
    "37057": "Davidson", "37059": "Davie", "37061": "Duplin", "37063": "Durham",
    "37065": "Edgecombe", "37067": "Forsyth", "37069": "Franklin", "37071": "Gaston",
    "37073": "Gates", "37075": "Graham", "37077": "Granville", "37079": "Greene",
    "37081": "Guilford", "37083": "Halifax", "37085": "Harnett", "37087": "Haywood",
    "37089": "Henderson", "37091": "Hertford", "37093": "Hoke", "37095": "Hyde",
    "37097": "Iredell", "37099": "Jackson", "37101": "Johnston", "37103": "Jones",
    "37105": "Lee", "37107": "Lenoir", "37109": "Lincoln", "37111": "McDowell",
    "37113": "Macon", "37115": "Madison", "37117": "Martin", "37119": "Mecklenburg",
    "37121": "Mitchell", "37123": "Montgomery", "37125": "Moore", "37127": "Nash",
    "37129": "New Hanover", "37131": "Northampton", "37133": "Onslow", "37135": "Orange",
    "37137": "Pamlico", "37139": "Pasquotank", "37141": "Pender", "37143": "Perquimans",
    "37145": "Person", "37147": "Pitt", "37149": "Polk", "37151": "Randolph",
    "37153": "Richmond", "37155": "Robeson", "37157": "Rockingham", "37159": "Rowan",
    "37161": "Rutherford", "37163": "Sampson", "37165": "Scotland", "37167": "Stanly",
    "37169": "Stokes", "37171": "Surry", "37173": "Swain", "37175": "Transylvania",
    "37177": "Tyrrell", "37179": "Union", "37181": "Vance", "37183": "Wake",
    "37185": "Warren", "37187": "Washington", "37189": "Watauga", "37191": "Wayne",
    "37193": "Wilkes", "37195": "Wilson", "37197": "Yadkin", "37199": "Yancey",
}

EMERGENCY_FACILITY_BACKUP: List[Dict[str, Any]] = [
    {"name": "East Chapel Hill High School", "county": "Orange", "lat": 35.9542, "lon": -79.0354, "max_wind_rating_mph": 120.0, "evacuation_capacity": 450, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Smith Middle School", "county": "Orange", "lat": 35.9427, "lon": -79.0805, "max_wind_rating_mph": 120.0, "evacuation_capacity": 350, "total_population": 0, "pet_friendly": False, "ada_accessible": True, "generator": True},
    {"name": "Durham County Memorial Stadium", "county": "Durham", "lat": 36.0345, "lon": -78.8920, "max_wind_rating_mph": 130.0, "evacuation_capacity": 800, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Southern Durham High School", "county": "Durham", "lat": 35.9320, "lon": -78.8576, "max_wind_rating_mph": 120.0, "evacuation_capacity": 500, "total_population": 0, "pet_friendly": False, "ada_accessible": True, "generator": True},
    {"name": "PNC Arena / Carter-Finley Complex", "county": "Wake", "lat": 35.8033, "lon": -78.7218, "max_wind_rating_mph": 150.0, "evacuation_capacity": 1500, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Southeast Raleigh Magnet High", "county": "Wake", "lat": 35.7533, "lon": -78.6012, "max_wind_rating_mph": 120.0, "evacuation_capacity": 600, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Greensboro Coliseum Complex", "county": "Guilford", "lat": 36.0594, "lon": -79.8258, "max_wind_rating_mph": 140.0, "evacuation_capacity": 2200, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Bojangles Coliseum Complex", "county": "Mecklenburg", "lat": 35.2045, "lon": -80.7972, "max_wind_rating_mph": 140.0, "evacuation_capacity": 2000, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "Trask Coliseum (UNCW)", "county": "New Hanover", "lat": 34.2257, "lon": -77.8763, "max_wind_rating_mph": 150.0, "evacuation_capacity": 1800, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "First Flight High School", "county": "Dare", "lat": 36.0185, "lon": -75.6713, "max_wind_rating_mph": 150.0, "evacuation_capacity": 700, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
    {"name": "WNC Agricultural Center", "county": "Buncombe", "lat": 35.4332, "lon": -82.5358, "max_wind_rating_mph": 130.0, "evacuation_capacity": 1200, "total_population": 0, "pet_friendly": True, "ada_accessible": True, "generator": True},
]

STATEWIDE_BCAT: Dict[str, Dict[str, str]] = {}
BCAT_SOURCE: str = "FEMA_BCAT_STATEWIDE_FALLBACK"

# ---------------------------------------------------------------------------
# In-Memory Cache
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
# Utility helpers
# ---------------------------------------------------------------------------

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _parse_bool(val: Any) -> bool:
    return str(val).strip().lower() in ("yes", "1", "true", "y")

def _extract_coord(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if not math.isnan(f) else None
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Statewide BCAT Ingestion
# ---------------------------------------------------------------------------

async def ingest_statewide_bcat() -> None:
    global STATEWIDE_BCAT, BCAT_SOURCE

    bcat_map: Dict[str, Dict[str, str]] = {
        fips: {
            "bcat_wind_resistance": "Resistant" if fips in ("37183", "37119", "37129", "37055") else "Not Resistant",
            "bcat_flood_resistance": "Resistant" if fips in ("37183", "37129", "37055") else "Not Resistant",
            "building_code_era": "2018 NC State Residential Code",
        }
        for fips in ALL_100_NC_COUNTIES
    }

    params = {
        "where": "STATE = 'NC'",
        "outFields": "COUNTY,COUNTY_FIPS,WIND_RESISTANT,FLOOD_RESISTANT,CODE_EDITION,BCAT_STATUS",
        "returnGeometry": "false",
        "resultRecordCount": 250,
        "f": "json",
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(FEMA_BCAT_URL, params=params)
            if resp.status_code == 200:
                features = resp.json().get("features", [])
                for feat in features:
                    attrs = feat.get("attributes", {})
                    fips = str(attrs.get("COUNTY_FIPS") or "").strip().zfill(5)
                    if fips in bcat_map:
                        w_res = str(attrs.get("WIND_RESISTANT", "")).lower()
                        f_res = str(attrs.get("FLOOD_RESISTANT", "")).lower()
                        bcat_map[fips] = {
                            "bcat_wind_resistance": "Resistant" if ("yes" in w_res or "resist" in w_res) else "Not Resistant",
                            "bcat_flood_resistance": "Resistant" if ("yes" in f_res or "resist" in f_res) else "Not Resistant",
                            "building_code_era": str(attrs.get("CODE_EDITION") or "2018 NC State Residential Code").strip(),
                        }
                STATEWIDE_BCAT = bcat_map
                BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_LIVE"
                return
        except Exception:
            pass

    if NC_BCAT_FALLBACK_PATH.exists():
        try:
            records = json.loads(NC_BCAT_FALLBACK_PATH.read_text(encoding="utf-8"))
            for rec in records:
                fips = str(rec.get("county_fips") or rec.get("COUNTY_FIPS") or "").strip().zfill(5)
                if len(fips) == 5:
                    bcat_map[fips] = {
                        "bcat_wind_resistance": rec.get("bcat_wind_resistance", "Not Resistant"),
                        "bcat_flood_resistance": rec.get("bcat_flood_resistance", "Not Resistant"),
                        "building_code_era": rec.get("building_code_era", "2018 NC State Residential Code"),
                    }
            STATEWIDE_BCAT = bcat_map
            BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_FALLBACK"
            return
        except Exception:
            pass

    STATEWIDE_BCAT = bcat_map
    BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_FALLBACK"

@asynccontextmanager
async def lifespan(_app: FastAPI):
    await ingest_statewide_bcat()
    yield

# ---------------------------------------------------------------------------
# App Setup & Schemas
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SafeHaven NC",
    description="Emergency resilience and adaptive shelter router — 100 NC counties.",
    version="3.6.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EvaluateRequest(BaseModel):
    lat: float
    lon: float
    year_built: int
    structure_type: str = Field(..., description="mobile_home | single_family | multi_family")
    scenario_wind_mph: Optional[float] = None
    max_radius_miles: Optional[float] = Field(default=DEFAULT_INITIAL_RADIUS_MILES, description="Initial shelter search radius in miles")

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
    bcat_rating: str
    shelter_source: str
    effective_search_radius_miles: float

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
# Resolvers & Normalizers
# ---------------------------------------------------------------------------

async def resolve_county(lat: float, lon: float) -> Tuple[Dict[str, str], str]:
    cache_key = f"c_{round(lat, 2)}_{round(lon, 2)}"
    cached = cache_get(cache_key)
    if cached:
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
            data = resp.json()
            features = data.get("features", [])
            if features:
                attrs = features[0]["attributes"]
                bare = str(attrs.get("NAME", "Orange")).strip()
                fips = str(attrs.get("GEOID", "37135")).strip().zfill(5)
                name = bare if "county" in bare.lower() else f"{bare} County"
                res = {"county_name": name, "county_fips": fips}
                cache_set(cache_key, res, TTL_COUNTY)
                return res, "US_CENSUS_TIGERWEB_LIVE"
        except Exception:
            pass

    return {"county_name": "Orange County", "county_fips": "37135"}, "CENSUS_FALLBACK"

async def fetch_weather(lat: float, lon: float) -> WeatherInfo:
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
            data = resp.json()
            curr = data.get("current_weather", {})
            hourly = data.get("hourly", {})
            gusts = [float(g) for g in hourly.get("wind_gusts_10m", []) if g is not None]
            precip = [float(p) for p in hourly.get("precipitation", []) if p is not None]
            return WeatherInfo(
                current_temp_f=round(float(curr.get("temperature", 72.0)), 1),
                wind_gusts_mph=round(max(gusts) if gusts else float(curr.get("windspeed", 15.0)), 1),
                sustained_wind_mph=round(float(curr.get("windspeed", 10.0)), 1),
                precipitation_next_24h_in=round(sum(precip), 2),
            )
        except Exception:
            return WeatherInfo(
                current_temp_f=70.0,
                wind_gusts_mph=15.0,
                sustained_wind_mph=8.0,
                precipitation_next_24h_in=0.0,
            )

def _normalize_raw_shelter(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    props = item.get("properties") or item.get("attributes") or item
    if not isinstance(props, dict):
        props = item

    geom = item.get("geometry") if isinstance(item.get("geometry"), dict) else {}
    coords = geom.get("coordinates") if isinstance(geom.get("coordinates"), list) else []

    lat: Optional[float] = None
    lon: Optional[float] = None

    if len(coords) >= 2:
        lon = _extract_coord(coords[0])
        lat = _extract_coord(coords[1])

    if lat is None or lon is None:
        if "y" in geom and "x" in geom:
            lat = _extract_coord(geom["y"])
            lon = _extract_coord(geom["x"])

    if lat is None or lon is None:
        for k in ("lat", "LAT", "latitude", "Latitude", "LATITUDE", "y", "Y"):
            if k in props and props[k] is not None:
                lat = _extract_coord(props[k])
                if lat is not None:
                    break
        for k in ("lon", "LON", "longitude", "Longitude", "LONGITUDE", "long", "Long", "x", "X"):
            if k in props and props[k] is not None:
                lon = _extract_coord(props[k])
                if lon is not None:
                    break

    if lat is None or lon is None:
        return None

    name = (
        props.get("name")
        or props.get("SHELTER_NAME")
        or props.get("FACILITY_NAME")
        or props.get("FACILITY_N")
        or props.get("NAME")
        or "Emergency Shelter"
    )

    county = (
        props.get("county")
        or props.get("COUNTY")
        or props.get("COUNTY_NAME")
        or props.get("COUNTY_NAM")
        or props.get("JURISDICTION")
        or "NC"
    )

    raw_rating = props.get("max_wind_rating_mph") or props.get("WIND_RATING_MPH") or props.get("WIND_RATING")
    try:
        rating = float(raw_rating) if raw_rating and float(raw_rating) > 0 else NSS_DEFAULT_WIND_RATING_MPH
    except (ValueError, TypeError):
        rating = NSS_DEFAULT_WIND_RATING_MPH

    cap = int(
        props.get("evacuation_capacity")
        or props.get("EVACUATION_CAPACITY")
        or props.get("CAPACITY")
        or props.get("capacity")
        or 400
    )
    pop = int(props.get("total_population") or props.get("TOTAL_POPULATION") or props.get("POPULATION") or 0)
    status = str(props.get("shelter_status") or props.get("SHELTER_STATUS") or "DESIGNATED").upper()

    return {
        "name": str(name).strip(),
        "county": str(county).strip(),
        "lat": lat,
        "lon": lon,
        "max_wind_rating_mph": rating,
        "evacuation_capacity": cap,
        "total_population": pop,
        "remaining_capacity": max(0, cap - pop),
        "shelter_status": status,
        "pet_friendly": bool(props.get("pet_friendly", False) or _parse_bool(props.get("PET_FRIENDLY"))),
        "ada_accessible": bool(props.get("ada_accessible", True) or _parse_bool(props.get("ADA_COMPLIANT"))),
        "generator": bool(props.get("generator", True) or _parse_bool(props.get("GENERATOR_ON_SITE"))),
    }

async def fetch_shelters() -> Tuple[List[Dict[str, Any]], str]:
    cached = cache_get("nss_shelters")
    if cached and len(cached[0]) > 0:
        return cached

    shelters: List[Dict[str, Any]] = []
    source = "FEMA_NSS_LIVE"

    params = {
        "where": "STATE = 'NC'",
        "outFields": "*",
        "f": "geojson",
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        try:
            resp = await client.get(NSS_SHELTERS_URL, params=params)
            if resp.status_code == 200:
                features = resp.json().get("features", [])
                for feat in features:
                    norm = _normalize_raw_shelter(feat)
                    if norm:
                        shelters.append(norm)
        except Exception:
            pass

    if not shelters and NC_SHELTERS_FALLBACK_PATH.exists():
        source = "LOCAL_STATEWIDE_INVENTORY"
        try:
            raw_text = NC_SHELTERS_FALLBACK_PATH.read_text(encoding="utf-8")
            raw_data = json.loads(raw_text)

            items = []
            if isinstance(raw_data, dict):
                items = raw_data.get("features", []) or raw_data.get("records", []) or [raw_data]
            elif isinstance(raw_data, list):
                items = raw_data

            for item in items:
                norm = _normalize_raw_shelter(item)
                if norm:
                    shelters.append(norm)
        except Exception as exc:
            print(f"[SafeHaven] Error parsing nc_shelters.json: {exc}")

    if not shelters:
        source = "INTERNAL_EMERGENCY_BACKUP"
        shelters = [
            {
                **s,
                "remaining_capacity": max(0, s["evacuation_capacity"] - s["total_population"]),
                "shelter_status": "DESIGNATED",
            }
            for s in EMERGENCY_FACILITY_BACKUP
        ]

    cache_set("nss_shelters", (shelters, source), TTL_SHELTERS)
    return shelters, source

# ---------------------------------------------------------------------------
# Vulnerability Scoring & Adaptive Routing Calculations
# ---------------------------------------------------------------------------

def calculate_vulnerability(
    year_built: int,
    structure_type: str,
    bcat: Dict[str, str],
    storm_wind_mph: float,
) -> Dict[str, Any]:
    multiplier = STRUCTURE_MULTIPLIERS.get(structure_type, 1.0)
    base_value = STRUCTURE_BASE_VALUE_USD.get(structure_type, 300_000)
    non_resistant = bcat.get("bcat_wind_resistance") == "Not Resistant"
    bcat_mod = 1.15 if non_resistant else 1.0

    if storm_wind_mph < 40.0:
        damage_fraction = 0.0
    elif storm_wind_mph < 74.0:
        t = (storm_wind_mph - 40.0) / 34.0
        damage_fraction = 0.03 * t
    elif storm_wind_mph < 96.0:
        t = (storm_wind_mph - 74.0) / 22.0
        damage_fraction = 0.04 + 0.11 * t
    elif storm_wind_mph < 111.0:
        t = (storm_wind_mph - 96.0) / 15.0
        damage_fraction = 0.15 + 0.15 * t
    else:
        t = min(1.0, (storm_wind_mph - 111.0) / 30.0)
        damage_fraction = 0.30 + 0.40 * t

    adj_fraction = min(1.0, damage_fraction * multiplier * bcat_mod)
    pred_damage = round(base_value * adj_fraction, 2)

    if storm_wind_mph < 40.0:
        raw = (storm_wind_mph / 40.0) * 12.0
        if year_built < 2000:
            raw += 2.0
        score = int(round(min(19.0, max(0.0, raw))))
    elif storm_wind_mph < 74.0:
        t = (storm_wind_mph - 40.0) / 34.0
        raw = 20.0 + 20.0 * t + ((multiplier - 1.0) * 8.0)
        if year_built < 2000:
            raw += 4.0
        score = int(round(min(49.0, max(20.0, raw))))
    else:
        t = min(1.0, (storm_wind_mph - 74.0) / 56.0)
        raw = 50.0 + 35.0 * t + ((multiplier - 1.0) * 12.0)
        if year_built < 2000:
            raw += 6.0
        if non_resistant:
            raw += 4.0
        score = int(round(min(100.0, max(50.0, raw))))

    return {"vulnerability_score": score, "predicted_damage_usd": pred_damage}

def build_recommendation(
    score: int,
    county_name: str,
    bcat: Dict[str, str],
    shelters_found: int,
    storm_wind_mph: float,
    effective_radius_miles: float,
    initial_radius_miles: float,
) -> str:
    if score >= 75:
        risk = "EVACUATE"
        action = "Evacuate immediately to a rated emergency shelter. High risk of catastrophic structural failure."
    elif score >= 50:
        risk = "HIGH RISK"
        action = "Significant structural threat detected. Strongly consider relocating to a rated shelter."
    elif score >= 20:
        risk = "MODERATE RISK"
        action = "Elevated wind conditions. Code-compliant structures may shelter-in-place; monitor local alerts."
    else:
        return (
            f"[LOW RISK] Current wind conditions pose minimal structural threat; no evacuation required. "
            f"Note: {county_name} maintains a '{bcat.get('bcat_wind_resistance')}' FEMA BCAT wind rating "
            f"under the {bcat.get('building_code_era')} standard."
        )

    code_note = f" {county_name} building codes are rated '{bcat.get('bcat_wind_resistance')}'."

    if shelters_found > 0:
        if effective_radius_miles > initial_radius_miles:
            shelter_note = (
                f" Notice: No open facilities were found within {int(initial_radius_miles)} miles. "
                f"Expanded search to {int(effective_radius_miles)} miles identified {shelters_found} regional shelter(s)."
            )
        else:
            shelter_note = f" {shelters_found} designated shelter(s) available within {int(effective_radius_miles)} miles."
    elif storm_wind_mph >= 74.0:
        shelter_note = f" WARNING: No rated shelters within {int(effective_radius_miles)} miles meet the {int(storm_wind_mph)} mph wind threshold. Contact county emergency management."
    else:
        shelter_note = f" No designated shelters located within {int(effective_radius_miles)} miles. Contact local emergency management."

    return f"[{risk}] {action}{code_note}{shelter_note}"

def find_survivable_shelters_adaptive(
    lat: float,
    lon: float,
    storm_wind_mph: float,
    all_shelters: List[Dict[str, Any]],
    initial_radius_miles: float = DEFAULT_INITIAL_RADIUS_MILES,
    max_expanded_radius_miles: float = MAX_EXPANDED_RADIUS_MILES,
    limit: int = MAX_SHELTERS_RETURNED,
) -> Tuple[List[ShelterResult], float]:
    """
    Progressively searches concentric rings (initial -> 50mi -> 100mi)
    to ensure rural or edge-of-county locations receive a survivable shelter.
    """
    survivable = [
        s for s in all_shelters
        if float(s.get("max_wind_rating_mph") or NSS_DEFAULT_WIND_RATING_MPH) >= storm_wind_mph
    ]

    candidates = []
    for s in survivable:
        dist = haversine_miles(lat, lon, s["lat"], s["lon"])
        candidates.append((dist, s))

    candidates.sort(key=lambda pair: pair[0])

    # Distinct progressive search radii
    tiers = [initial_radius_miles]
    if initial_radius_miles < 50.0:
        tiers.append(50.0)
    if max_expanded_radius_miles > tiers[-1]:
        tiers.append(max_expanded_radius_miles)

    matched = []
    effective_radius = tiers[-1]

    for tier in tiers:
        in_tier = [pair for pair in candidates if pair[0] <= tier]
        if in_tier:
            matched = in_tier
            effective_radius = tier
            break

    results = []
    for dist, s in matched[:limit]:
        rating = float(s.get("max_wind_rating_mph") or NSS_DEFAULT_WIND_RATING_MPH)
        results.append(
            ShelterResult(
                name=s["name"],
                county=s["county"],
                max_wind_rating_mph=rating,
                evacuation_capacity=int(s.get("evacuation_capacity", 400)),
                remaining_capacity=int(s.get("remaining_capacity", 400)),
                total_population=int(s.get("total_population", 0)),
                shelter_status=s.get("shelter_status", "OPEN"),
                pet_friendly=bool(s.get("pet_friendly", False)),
                ada_accessible=bool(s.get("ada_accessible", True)),
                generator=bool(s.get("generator", True)),
                distance_miles=round(dist, 2),
                lat=s["lat"],
                lon=s["lon"],
                navigation_url=f"https://www.google.com/maps/dir/?api=1&destination={s['lat']},{s['lon']}",
            )
        )

    return results, effective_radius

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {"service": "SafeHaven NC API", "version": "3.6.0", "status": "online"}

@app.get("/health")
async def health():
    return {"status": "ok", "bcat_source": BCAT_SOURCE, "counties_indexed": len(STATEWIDE_BCAT)}

@app.get("/api/shelters")
async def get_shelters():
    shelters, _ = await fetch_shelters()
    return shelters

@app.get("/api/debug-shelters")
def debug_shelters():
    file_exists = NC_SHELTERS_FALLBACK_PATH.exists()
    file_size = os.path.getsize(NC_SHELTERS_FALLBACK_PATH) if file_exists else 0
    raw_preview = ""
    parsed_count = 0
    parse_error = None
    if file_exists:
        try:
            content = NC_SHELTERS_FALLBACK_PATH.read_text(encoding="utf-8")
            raw_preview = content[:300]
            data = json.loads(content)
            items = data.get("features", data) if isinstance(data, dict) else data
            parsed_count = len(items) if isinstance(items, list) else 1
        except Exception as e:
            parse_error = str(e)
    return {
        "file_path": str(NC_SHELTERS_FALLBACK_PATH),
        "file_exists": file_exists,
        "file_size_bytes": file_size,
        "items_in_raw_json": parsed_count,
        "parse_error": parse_error,
        "raw_preview": raw_preview,
    }

@app.post("/api/evaluate", response_model=EvaluateResponse)
async def evaluate(payload: EvaluateRequest) -> EvaluateResponse:
    county_res, (weather, shelter_res) = await asyncio.gather(
        resolve_county(payload.lat, payload.lon),
        asyncio.gather(
            fetch_weather(payload.lat, payload.lon),
            fetch_shelters(),
        ),
    )

    county, county_source = county_res
    all_shelters, shelter_source = shelter_res

    fips = county["county_fips"]
    bcat = STATEWIDE_BCAT.get(fips, {
        "bcat_wind_resistance": "Not Resistant",
        "bcat_flood_resistance": "Not Resistant",
        "building_code_era": "2018 NC State Residential Code",
    })

    storm_wind_mph = float(payload.scenario_wind_mph) if payload.scenario_wind_mph is not None else weather.wind_gusts_mph
    initial_radius = float(payload.max_radius_miles) if payload.max_radius_miles is not None else DEFAULT_INITIAL_RADIUS_MILES

    vuln = calculate_vulnerability(payload.year_built, payload.structure_type, bcat, storm_wind_mph)
    shelters, effective_radius = find_survivable_shelters_adaptive(
        lat=payload.lat,
        lon=payload.lon,
        storm_wind_mph=storm_wind_mph,
        all_shelters=all_shelters,
        initial_radius_miles=initial_radius,
        max_expanded_radius_miles=MAX_EXPANDED_RADIUS_MILES,
        limit=MAX_SHELTERS_RETURNED,
    )
    recommendation = build_recommendation(
        score=vuln["vulnerability_score"],
        county_name=county["county_name"],
        bcat=bcat,
        shelters_found=len(shelters),
        storm_wind_mph=storm_wind_mph,
        effective_radius_miles=effective_radius,
        initial_radius_miles=initial_radius,
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
        survivable_shelters=shelters,
        data_sources=DataSources(
            county_resolution=county_source,
            bcat_rating=BCAT_SOURCE,
            shelter_source=shelter_source,
            effective_search_radius_miles=effective_radius,
        ),
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
