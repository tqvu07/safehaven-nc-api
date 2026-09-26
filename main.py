"""
SafeHaven NC — Emergency Resilience and Adaptive Shelter Router
Version 3.3.0 — Fully Database & Dataset Driven
Carolina Data Challenge
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

# ---------------------------------------------------------------------------
# Dynamic Statewide BCAT Ingestion
# ---------------------------------------------------------------------------

async def ingest_statewide_bcat() -> None:
    global STATEWIDE_BCAT, BCAT_SOURCE

    bcat_map: Dict[str, Dict[str, str]] = {}

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
                    if len(fips) == 5 and fips != "00000":
                        w_res = str(attrs.get("WIND_RESISTANT", "")).lower()
                        f_res = str(attrs.get("FLOOD_RESISTANT", "")).lower()
                        bcat_map[fips] = {
                            "bcat_wind_resistance": "Resistant" if ("yes" in w_res or "resist" in w_res) else "Not Resistant",
                            "bcat_flood_resistance": "Resistant" if ("yes" in f_res or "resist" in f_res) else "Not Resistant",
                            "building_code_era": str(attrs.get("CODE_EDITION") or "2018 NC State Residential Code").strip(),
                        }
                if bcat_map:
                    STATEWIDE_BCAT = bcat_map
                    BCAT_SOURCE = "FEMA_BCAT_STATEWIDE_LIVE"
                    print(f"[SafeHaven] Loaded {len(STATEWIDE_BCAT)} counties from live FEMA BCAT API.")
                    return
        except Exception as exc:
            print(f"[SafeHaven] Live FEMA BCAT query failed ({exc}). Trying local cache file...")

    # Fallback to local json if available
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
            print(f"[SafeHaven] Loaded {len(STATEWIDE_BCAT)} counties from nc_bcat_statewide.json.")
        except Exception as e:
            print(f"[SafeHaven] Error reading nc_bcat_statewide.json: {e}")

@asynccontextmanager
async def lifespan(_app: FastAPI):
    await ingest_statewide_bcat()
    yield

# ---------------------------------------------------------------------------
# App Setup & Models
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SafeHaven NC",
    description="Emergency resilience and adaptive shelter router — 100 NC counties.",
    version="3.3.0",
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
# External Resolvers & Dynamic Shelter Normalizer
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

def _normalize_raw_shelter(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Flexible parser: handles standard dicts, GeoJSON features, and mixed casings."""
    props = item.get("properties", item)
    geom = item.get("geometry", {})
    coords = geom.get("coordinates", [])

    lat = None
    lon = None

    if len(coords) >= 2 and coords[0] is not None and coords[1] is not None:
        lon, lat = float(coords[0]), float(coords[1])
    else:
        for lat_key in ("lat", "LAT", "latitude", "Latitude", "y", "Y"):
            if lat_key in props and props[lat_key] is not None:
                lat = float(props[lat_key])
                break
        for lon_key in ("lon", "LON", "longitude", "Longitude", "x", "X"):
            if lon_key in props and props[lon_key] is not None:
                lon = float(props[lon_key])
                break

    if lat is None or lon is None:
        return None

    name = props.get("name") or props.get("SHELTER_NAME") or props.get("FACILITY_NAME") or "Emergency Shelter"
    county = props.get("county") or props.get("COUNTY") or props.get("COUNTY_NAME") or "Unknown"
    
    # Wind rating: if missing or 0, default to institutional building code rating
    raw_rating = props.get("max_wind_rating_mph") or props.get("WIND_RATING_MPH")
    try:
        rating = float(raw_rating) if raw_rating and float(raw_rating) > 0 else NSS_DEFAULT_WIND_RATING_MPH
    except (ValueError, TypeError):
        rating = NSS_DEFAULT_WIND_RATING_MPH

    evac_cap = int(props.get("evacuation_capacity") or props.get("EVACUATION_CAPACITY") or props.get("capacity") or 400)
    total_pop = int(props.get("total_population") or props.get("TOTAL_POPULATION") or 0)
    status = str(props.get("shelter_status") or props.get("SHELTER_STATUS") or "DESIGNATED").upper()

    return {
        "name": str(name).strip(),
        "county": str(county).strip(),
        "lat": lat,
        "lon": lon,
        "max_wind_rating_mph": rating,
        "evacuation_capacity": evac_cap,
        "total_population": total_pop,
        "remaining_capacity": max(0, evac_cap - total_pop),
        "shelter_status": status,
        "pet_friendly": _parse_bool(props.get("pet_friendly") if "pet_friendly" in props else props.get("PET_FRIENDLY")),
        "ada_accessible": _parse_bool(props.get("ada_accessible") if "ada_accessible" in props else props.get("ADA_COMPLIANT", True)),
        "generator": _parse_bool(props.get("generator") if "generator" in props else props.get("GENERATOR_ON_SITE", True)),
    }

async def fetch_shelters() -> Tuple[List[Dict[str, Any]], str]:
    cached = cache_get("nss_shelters")
    if cached:
        return cached

    shelters: List[Dict[str, Any]] = []
    source = "FEMA_NSS_LIVE"

    params = {
        "where": "STATE = 'NC'",
        "outFields": "SHELTER_NAME,CITY,COUNTY,EVACUATION_CAPACITY,TOTAL_POPULATION,SHELTER_STATUS,PET_FRIENDLY,ADA_COMPLIANT,GENERATOR_ON_SITE",
        "f": "geojson",
    }

    # Step 1: Query live FEMA NSS feed
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

    # Step 2: Fall back to nc_shelters.json
    if not shelters and NC_SHELTERS_FALLBACK_PATH.exists():
        source = "LOCAL_STATEWIDE_INVENTORY"
        try:
            raw_data = json.loads(NC_SHELTERS_FALLBACK_PATH.read_text(encoding="utf-8"))
            items = raw_data.get("features", raw_data) if isinstance(raw_data, dict) else raw_data
            for raw_item in items:
                norm = _normalize_raw_shelter(raw_item)
                if norm:
                    shelters.append(norm)
            print(f"[SafeHaven] Ingested {len(shelters)} shelters from nc_shelters.json")
        except Exception as e:
            print(f"[SafeHaven] Failed to read nc_shelters.json: {e}")

    cache_set("nss_shelters", (shelters, source), TTL_SHELTERS)
    return shelters, source

# ---------------------------------------------------------------------------
# Vulnerability and Routing Math
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
        shelter_note = f" {shelters_found} designated shelter(s) available nearby."
    elif storm_wind_mph >= 74.0:
        shelter_note = " WARNING: No nearby shelters meet the peak wind threshold. Contact county emergency management."
    else:
        shelter_note = " Contact local emergency management for regional sites."

    return f"[{risk}] {action}{code_note}{shelter_note}"

def find_survivable_shelters(
    lat: float,
    lon: float,
    storm_wind_mph: float,
    all_shelters: List[Dict[str, Any]],
    limit: int = MAX_SHELTERS_RETURNED,
) -> List[ShelterResult]:
    # Shelters survive if their design rating meets or exceeds the storm wind
    survivable = [
        s for s in all_shelters
        if float(s.get("max_wind_rating_mph") or NSS_DEFAULT_WIND_RATING_MPH) >= storm_wind_mph
    ]

    ranked = []
    for s in survivable:
        dist = haversine_miles(lat, lon, s["lat"], s["lon"])
        ranked.append(
            ShelterResult(
                name=s["name"],
                county=s["county"],
                max_wind_rating_mph=float(s.get("max_wind_rating_mph") or NSS_DEFAULT_WIND_RATING_MPH),
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

    ranked.sort(key=lambda x: x.distance_miles)
    return ranked[:limit]

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {"service": "SafeHaven NC API", "version": "3.3.0", "status": "online"}

@app.get("/health")
async def health():
    return {"status": "ok", "bcat_source": BCAT_SOURCE, "counties_indexed": len(STATEWIDE_BCAT)}

@app.get("/api/shelters")
async def get_shelters():
    shelters, _ = await fetch_shelters()
    return shelters

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
    vuln = calculate_vulnerability(payload.year_built, payload.structure_type, bcat, storm_wind_mph)
    shelters = find_survivable_shelters(payload.lat, payload.lon, storm_wind_mph, all_shelters)
    recommendation = build_recommendation(vuln["vulnerability_score"], county["county_name"], bcat, len(shelters), storm_wind_mph)

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
        ),
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
