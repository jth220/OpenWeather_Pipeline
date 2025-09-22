from datetime import datetime, timezone, date
import hashlib
import json
from json import JSONDecodeError
from collections import defaultdict
import gzip
from typing import Tuple, List, Dict
import boto3
from botocore.exceptions import ClientError
import argparse
import sys
import os
from dotenv import load_dotenv
load_dotenv("settings.env")


OWM_API_KEY = os.environ["OWM_API_KEY"]
CITY_NAME = os.environ.get("CITY_NAME", "London")
MAX_PAYLOAD_BYTES = int(os.environ.get("MAX_PAYLOAD_BYTES","5242880"))
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BRONZE_BUCKET= os.environ.get("BRONZE_BUCKET")
QUARANTINE_BUCKET=os.environ.get("QUARANTINE_BUCKET")


"""
Silver Transform for OpenWeather "current weather" events.

Purpose
-------
Promote immutable Bronze JSONL events into a clean, typed, de-duplicated
Silver dataset with explicit quality rules and strong provenance.

Scope
-----
- Input: Bronze objects written by `bronze_ingest.py`, one JSON object per line.
- Output: Partitioned Parquet (or warehouse table) containing one row per
  (city_id, event_ts_utc) with standardized columns and validation flags.

Primary Key
-----------
(city_id, event_ts_utc) per provider ("openweather").

Contract (Columns)
------------------
Keys & Provenance:
- event_id: str (24-hex, deterministic from provider natural key)
- city_id: int
- city_slug: str
- event_ts_utc: timestamp (tz-aware UTC)
- ingested_at_utc: timestamp (tz-aware UTC at promotion time)
- bronze_object_key: str (pointer to raw)
- checksum_sha256: str (integrity of raw JSONL object)
- source: str ("openweather")
- quality_flags: list[str] (or semicolon-joined string)
- conflict: bool (true if a competing record was dropped during dedup)

Core Measurements:
- temp_c, feels_like_c, temp_min_c, temp_max_c: float
- pressure_hpa: int
- humidity_pct: int
- wind_speed_ms, wind_gust_ms: float | null
- wind_deg: int | null
- clouds_pct: int | null
- visibility_m: int | null
- rain_1h_mm, snow_1h_mm: float | null
- weather_code: int | null
- weather_main, weather_desc: str | null

Geo & Time:
- lat: float
- lon: float
- timezone_offset_s: int | null
- date_utc: date  (derived from event_ts_utc)
- hour_utc: int   (0-23; optional partition)

Validation Rules
----------------
Critical (quarantine on failure):
- city_id not null
- event_ts_utc not null and >= 2000-01-01 UTC
- temp_c not null

Non-critical (nullify + flag on violation):
- temp_c ∈ [-80, 65]
- pressure_hpa ∈ [850, 1100]
- humidity_pct ∈ [0, 100]
- wind_speed_ms, wind_gust_ms ≥ 0
- clouds_pct ∈ [0, 100]
- lat ∈ [-90, 90], lon ∈ [-180, 180]
- visibility_m ≥ 0 (flag if > 20_000 as suspicious_high_visibility)
- rain_1h_mm, snow_1h_mm ≥ 0  (negative → quarantine)

Deduplication Policy
--------------------
Uniqueness on (city_id, event_ts_utc). Keep LAST writer (or FIRST—choose and
encode consistently). If conflicting payloads exist, set conflict=true on survivor.

Partitioning
------------
- Always: date_utc
- Optional: hour_utc (if frequent hour-level filters)
- Directory layout for Parquet:
  silver/weather_current/date_utc=YYYY-MM-DD[/hour_utc=HH]/city_slug=<slug>/*.parquet

Idempotency & Side Effects
--------------------------
- Promotion is deterministic for identical Bronze inputs.
- Writing is idempotent within a partition if the same rows are produced.
- Quarantine writes mirror Bronze partitioning and include reason + context.

Configuration
-------------
Relies on environment variables for buckets/regions. No secrets are logged.
"""


def list_bronze_keys(city_slug: str, date_utc: date) -> list[str]:
 """
 List Bronze JSONL object keys for a given city and UTC date.

 Args:
  city_slug (str): Canonical slug for the city (e.g., "london").
  date_utc (datetime.date): Target UTC calendar date to scan under Bronze.

 Returns:
  list[str]: Fully qualified object keys (relative to the Bronze bucket)
             that end with ".jsonl" for the specified city/date. The list
             may be empty if no data is present.

 Raises:
  RuntimeError: On storage access errors (list operation failure).

 Notes:
  - This function performs discovery only (no reads); it may paginate under
    the hood.
  - If hour-level partitioning exists in Bronze, keys will span multiple
    hour subdirectories.
  - Caller decides whether to further filter by hour(s).
 """
 bucket = BRONZE_BUCKET
 prefix_base = f"weather/city={city_slug}/date={date_utc.isoformat()}" #establishes parameters for prefixes
 client = boto3.client('s3', 
                      aws_access_key_id = AWS_ACCESS_KEY_ID,
                      aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                      region_name = "eu-north-1",
                      )

 keys: list[str] = []
 try:
        for hour in range(24):
            hour_prefix = f"{prefix_base}/hour={hour:02d}/" #padded with two digits
            paginator = client.get_paginator("list_objects_v2") #paginator is an iterator over pages of results
            for page in paginator.paginate(Bucket=bucket, Prefix=hour_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"] #Iterate through page contents
                    if key.endswith(".jsonl"):
                        keys.append(key) #Append key ()
 except ClientError as e:
        raise RuntimeError(f"S3 list failed: {e}")

 return keys


def read_bronze_jsonl(object_key: str) -> Tuple[List[Dict], str]:
 """
 Read a Bronze JSONL object and return parsed payloads.

 Args:
  object_key (str): Bronze object key previously discovered.

 Returns:
  tuple[list[dict], str]:
    - payloads: List of dicts parsed from each non-empty line in the object.
    - checksum_sha256: Hex digest of the raw object bytes, used for provenance.

 Raises:
  RuntimeError: If the object cannot be fetched or parsed.
  ValueError: If a line is not valid JSON.

 Idempotency:
  - Pure read. Does not mutate storage.

 Observability:
  - The returned checksum is used to verify integrity and assist dedup.
 """
 client = boto3.client('s3', 
                      aws_access_key_id = AWS_ACCESS_KEY_ID,
                      aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                      region_name = "eu-north-1",
                      )

 # --- 1) Fetch raw bytes from S3 ---
 try:
        resp = client.get_object(Bucket=BRONZE_BUCKET, Key=object_key)
        body = resp["Body"].read()  # bytes
 except ClientError as e:
        raise RuntimeError(f"Failed to fetch s3://{BRONZE_BUCKET}/{object_key}: {e}")

 # --- 2) Compute checksum of RAW bytes (compressed if stored compressed) ---
 checksum = hashlib.sha256(body).hexdigest()

 # --- 3) Decompress if needed (prefer ContentEncoding; fallback by extension) ---
 content_encoding = (resp.get("ContentEncoding") or "").lower()
 is_gzip = content_encoding == "gzip" or object_key.endswith(".gz")
 if is_gzip:
        try:
            body = gzip.decompress(body)
        except OSError as e:
            raise RuntimeError(
                f"Object appears gzip-encoded but could not be decompressed: {e}"
            )

 # --- 4) Decode to text (UTF-8) ---
 try:
        text = body.decode("utf-8")
 except UnicodeDecodeError as e:
        raise RuntimeError(f"UTF-8 decode failed for {object_key}: {e}")

 # --- 5) Parse JSON Lines ---
 payloads: List[Dict] = []
 for i, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue  # skip blank lines
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            # Include line number and a short preview to aid debugging.
            preview = (line[:120] + "…") if len(line) > 120 else line
            raise ValueError(
                f"Invalid JSON at line {i} in {object_key}: {e.msg}. Line preview: {preview}"
            )
        if not isinstance(obj, dict):
            # If you expect only dicts, enforce it. Remove this if arrays/values are valid for your pipeline.
            raise ValueError(
                f"Expected a JSON object at line {i} in {object_key}, got {type(obj).__name__}."
            )
        payloads.append(obj)

 return payloads, checksum

def normalize(payload, *, city_slug, bronze_object_key, checksum_sha256):
 """
 Map a raw OpenWeather payload into a Silver row (no validation yet).

 Args:
  payload (dict): Single OpenWeather "current weather" JSON object.
  city_slug (str): Canonical city slug for partitioning.
  bronze_key (str): Source Bronze object key (provenance).
  checksum_sha256 (str): Hex digest of the Bronze object bytes.

 Returns:
  dict | None: A single Silver-shaped row with all expected columns,
               including partitions and provenance. Returns None if the
               payload lacks natural key fields required to derive:
               - city_id (payload['id'])
               - event_ts_utc (from payload['dt'])
               In this case, caller should quarantine with reason
               "missing_natural_key".

 Columns Populated:
  - event_id derived deterministically from (provider, city_id, dt)
  - city_id, city_slug, city_name (title-cased if present)
  - event_ts_utc (tz-aware UTC)
  - ingested_at_utc (current UTC now at normalization time)
  - bronze_object_key, checksum_sha256, source="openweather"
  - All core measurements & geo fields where present
  - date_utc, hour_utc derived from event_ts_utc
  - quality_flags initialized empty
  - conflict initialized False

 Determinism:
  - Given identical payload, city_slug, bronze_key, checksum → identical row.

 Side Effects:
  - None. Pure transformation.
 """

 try:
    city_id = int(payload["id"])
    dt = int(payload["dt"])
 except (KeyError, TypeError, ValueError): #Checks for missing id and dt
    return None #Upstream quarantine from here
 
 ts_utc = datetime.fromtimestamp(dt, timezone.utc) #Converts unix timestamp to a datetime object in UTC
 ts_utc_date, ts_utc_HR = ts_utc.date().isoformat(), ts_utc.hour  #Derives a date and an hour of retrieval
 
 raw_key = (f"owm:{city_id}:{dt}") #
 event_id_hex24 = hashlib.sha1(raw_key.encode()).hexdigest()[:24]

 # --- Safe sub-dicts --- Extract these from the json file
 main = payload.get("main", {}) #Empty brackets are to indicate optional brackets incase if nothing is found, it will return a null instead
 wind = payload.get("wind", {})
 clouds = payload.get("clouds", {})
 rain = payload.get("rain", {})
 snow = payload.get("snow", {})
 weather_list = payload.get("weather", [])

  #--- Geo / time ---
 lat = payload.get("coord", {}).get("lat")
 lon = payload.get("coord", {}).get("lon")
 timezone_offset_s = payload.get("timezone")

 # --- City naming ---
 city_name = payload.get("name")
 # city_slug comes from the function arg

 # --- Main block ---
 temp_c = main.get("temp")
 feels_like_c = main.get("feels_like")
 temp_min_c = main.get("temp_min")
 temp_max_c = main.get("temp_max")
 pressure_hpa = main.get("pressure")
 humidity_pct = main.get("humidity")

 # --- Wind ---
 wind_speed_ms = wind.get("speed")
 wind_deg = wind.get("deg")
 wind_gust_ms = wind.get("gust")

 # --- Clouds / visibility / precip ---
 clouds_pct = clouds.get("all")
 visibility_m = payload.get("visibility")
 rain_1h_mm = rain.get("1h")
 snow_1h_mm = snow.get("1h")

 # --- Weather descriptor (first element only) ---
 if weather_list:
    weather_code = weather_list[0].get("id")
    weather_main = weather_list[0].get("main")
    weather_desc = weather_list[0].get("description")
 else:
    weather_code = None
    weather_main = None
    weather_desc = None

 #With the extracted variables, convert it into a dictionary
 row = {
    # Keys & provenance
    "event_id": event_id_hex24,
    "city_id": city_id,
    "city_slug": city_slug,
    "city_name": city_name,
    "event_ts_utc": ts_utc,
    "ingested_at_utc": datetime.now(timezone.utc),
    "bronze_object_key": bronze_object_key,
    "checksum_sha256": checksum_sha256,
    "source": "openweather",

    # Partitions
    "date_utc": ts_utc_date,
    "hour_utc": ts_utc_HR,
    # Core metrics
    "temp_c": temp_c,
    "feels_like_c": feels_like_c,
    "temp_min_c": temp_min_c,
    "temp_max_c": temp_max_c,
    "pressure_hpa": pressure_hpa,
    "humidity_pct": humidity_pct,
    "wind_speed_ms": wind_speed_ms,
    "wind_deg": wind_deg,
    "wind_gust_ms": wind_gust_ms,
    "clouds_pct": clouds_pct,
    "visibility_m": visibility_m,
    "rain_1h_mm": rain_1h_mm,
    "snow_1h_mm": snow_1h_mm,

    # Descriptors
    "weather_code": weather_code,
    "weather_main": weather_main,
    "weather_desc": weather_desc,

    # Geo/time
    "lat": lat,
    "lon": lon,
    "timezone_offset_s": timezone_offset_s,

    # Quality & dedup markers (start empty)
    "quality_flags": "",
    "conflict": False,
 }
 return row
 



def validate(row):

 """
 Apply Silver validation rules to a single normalized row.

 Args:
  row (dict): A normalized Silver-shaped row (from `normalize`).

 Returns:
  tuple[dict, list[str], str]:
    - fixed_row: The row after applying nullifications/caps for non-critical
                 violations (do not silently cap unless the contract allows it;
                 prefer NULL + flag).
    - flags: List of quality flags applied to the row (strings).
    - verdict: One of {"pass", "quarantine"} indicating promotion eligibility.

 Validation Semantics:
  - Critical failures (e.g., missing event_ts_utc, temp_c, or nonsensical time)
    → verdict="quarantine".
  - Non-critical range violations → set offending field to NULL and append a
    descriptive flag (e.g., "humidity_out_of_range").
  - Negative accumulations (rain_1h_mm, snow_1h_mm) → quarantine.
  - Lat/Lon policy: either quarantine or null+flag, per contract decision.

 Invariants:
  - Primary key fields (city_id, event_ts_utc) must be present on "pass".
  - date_utc and hour_utc must be consistent with event_ts_utc.

 Side Effects:
  - None. Pure function.

 Examples of Flags:
  - "humidity_out_of_range"
  - "visibility_suspicious_high"
  - "latlon_out_of_range"
 """
 fixed = dict(row)  # shallow copy, however during critical, use the actual row for quick bails, otherwise for non critical check we use copies to safely mutate.
 flags = []

    # --- Critical checks ---
 if row.get("city_id") is None or row.get("event_ts_utc") is None: #Checks for every critical fields
        flags.append("missing_event_ts_utc_or_city_id")
        return fixed, flags, "quarantine" #Returns immediately if checks failso 

 if row.get("temp_c") is None:
        flags.append("missing_temp_c")
        return fixed, flags, "quarantine"

 if not isinstance(row["event_ts_utc"], datetime):
        flags.append("event_ts_not_datetime")
        return fixed, flags, "quarantine"

 if row["event_ts_utc"] < datetime(2000,1,1,tzinfo=timezone.utc): #Rule violation checks
        flags.append("event_ts_too_old")
        return fixed, flags, "quarantine"

    # continue with non-critical checks...
    #Temperature
 for f in ("temp_c", "feels_like_c", "temp_min_c", "temp_max_c"):
      v = fixed.get(f)
      if v is not None:
        if not isinstance(v, (int, float)):
          flags.append(f"{f}_is_not_numeric")
          fixed[f] = None

        elif not (-80<= v <=65):
          flags.append(f"{f}_is_out_of_range")
          fixed[f] = None

    # Pressure
 v = fixed.get("pressure_hpa")
 if v is not None:
     if not isinstance(v, (int, float)):
        flags.append("pressure_hpa_not_numeric")
        fixed["pressure_hpa"] = None
     elif not (850 <= v <= 1100):
        flags.append("pressure_hpa_out_of_range")
        fixed["pressure_hpa"] = None

    # Humidity
 v = fixed.get("humidity_pct")
 if v is not None:
     if not isinstance(v, (int, float)):
        flags.append("humidity_pct_not_numeric")
        fixed["humidity_pct"] = None
     elif not (0 <= v <= 100):
        flags.append("humidity_pct_out_of_range")
        fixed["humidity_pct"] = None

    # Wind speeds (speed, gust >= 0)
 for f in ("wind_speed_ms", "wind_gust_ms"):
     v = fixed.get(f)
     if v is None:
        continue
     if not isinstance(v, (int, float)):
        flags.append(f"{f}_not_numeric")
        fixed[f] = None
     elif v < 0:
        flags.append(f"{f}_negative")
        fixed[f] = None

    # Wind direction (0–360)
 v = fixed.get("wind_deg")
 if v is not None:
     if not isinstance(v, (int, float)):
        flags.append("wind_deg_not_numeric")
        fixed["wind_deg"] = None
     elif not (0 <= v <= 360):
        flags.append("wind_deg_out_of_range")
        fixed["wind_deg"] = None

    # Clouds
 v = fixed.get("clouds_pct")
 if v is not None:
      if not isinstance(v, (int, float)):
        flags.append("clouds_pct_not_numeric")
        fixed["clouds_pct"] = None
      elif not (0 <= v <= 100):
        flags.append("clouds_pct_out_of_range")
        fixed["clouds_pct"] = None

    # Visibility
 v = fixed.get("visibility_m")
 if v is not None:
     if not isinstance(v, (int, float)):
        flags.append("visibility_m_not_numeric")
        fixed["visibility_m"] = None
     elif v < 0:
        flags.append("visibility_m_negative")
        fixed["visibility_m"] = None
     elif v > 20000:
        flags.append("visibility_m_suspicious_high")

     # Precip accumulations
 for f in ("rain_1h_mm", "snow_1h_mm"):
  v = fixed.get(f)
  if v is None:
        continue
  if not isinstance(v, (int, float)):
        flags.append(f"{f}_not_numeric")
        fixed[f] = None
  elif v < 0:
        flags.append(f"{f}_negative")
        return fixed, flags, "quarantine"

      # Geo
 lat, lon = fixed.get("lat"), fixed.get("lon")

 bad_lat = (lat is not None) and (not isinstance(lat, (int, float)) or not (-90 <= lat <= 90))
 bad_lon = (lon is not None) and (not isinstance(lon, (int, float)) or not (-180 <= lon <= 180))

 if bad_lat:
       fixed["lat"] = None
       flags.append("latlon_out_of_range")

 if bad_lon:
       fixed["lon"] = None
       if "latlon_out_of_range" not in flags:
        flags.append("latlon_out_of_range")


 fixed["quality_flags"] = ";".join(flags) if flags else ""
 return fixed, flags, "pass"

#to add a quarantine
"""
Persist an invalid or rejected record into a quarantine area for audit.

Args:
  reason (str): Short machine-friendly reason (e.g., "missing_natural_key",
                "negative_rain", "critical_null_temp").
  payload (dict | str): The raw payload or a serialized form that caused
                        rejection (do NOT include secrets).
  context (dict): Additional fields to aid triage:
                  {
                    "city_slug": str,
                    "date_utc": str (YYYY-MM-DD),
                    "hour_utc": str (00-23),
                    "bronze_object_key": str,
                    "event_id": str | None,
                    "rule": str (optional human-readable message)
                  }

Returns:
  None

Raises:
  RuntimeError: On storage write failures.

Write Layout:
  quarantine/silver/reason=<reason>/city=<slug>/date=YYYY-MM-DD/hour=HH/<id>.json

Notes:
  - Keep files small and structured; avoid PII and secrets.
  - Quarantine volume is a signal—consider alerting if rate exceeds threshold.
"""


def process_microbatch(bronze_lines: list[str], city_slug:str , bronze_object_key:str , checksum_sha256:str , dedup_policy:str) -> tuple[list[dict], list[dict], list[dict]]:
 """
    Orchestrate a micro-batch from Bronze → Silver:
      JSONL lines (raw provider payloads) → normalize → validate → deduplicate.

    Inputs:
      bronze_lines:
        List of raw JSON strings. Each line should be one OpenWeather payload
        exactly as stored in Bronze (compact JSON per line).
      city_slug:
        Canonical city partition (e.g., "london"). Passed through to normalize().
      bronze_key:
        The S3 object key these lines came from (provenance).
      checksum_sha256:
        SHA-256 of the Bronze object (provenance). Same for all lines in this batch.
      dedup_policy:
        Deduplication policy; currently supports:
          - "last_write_wins": keep the row with the greatest ingested_at_utc,
            then break ties deterministically by (checksum_sha256, bronze_object_key, event_id).

    Returns:
      survivors:
        List of validated + deduplicated Silver rows. Exactly one per natural key
        (city_id, event_ts_utc). If a key had multiple candidates with differing
        content fingerprints, the survivor will have conflict=True and will include
        a 'duplicate_conflict' quality flag (appended to any existing flags).
      dups_meta:
        List of metadata records about dropped duplicates. Each item is:
          {
            "key": (city_id, event_ts_utc_iso),
            "survivor_event_id": str,
            "dropped_event_id": str,
            "reason": "identical" | "conflict",
            "policy": dedup_policy
          }
      rejected:
        List of rejects that did not pass validation. Each item is:
          {
            "reason": "missing_natural_key" | "validation_failed",
            "flags": list[str],
            "payload": dict,                 # the original provider payload
            "bronze_object_key": bronze_key,
            "checksum_sha256": checksum_sha256
          }

    Semantics:
      - normalize() may return None → treat as rejected with reason="missing_natural_key".
      - validate() returns (fixed, flags, verdict):
          • verdict="quarantine" → push to rejected with reason="validation_failed".
          • verdict="pass" → include 'fixed' row in candidate set.
      - deduplicate() runs over all passing rows, grouped by (city_id, event_ts_utc).
        • Identical fingerprints → keep one, others recorded as reason="identical".
        • Differing fingerprints → keep per policy, survivor.conflict=True and add
          "duplicate_conflict" to survivor's quality flags; dropped as reason="conflict".

    Invariants (assertions recommended post-run):
      - No two survivors share the same (city_id, event_ts_utc).
      - Every rejected record has a non-empty 'reason'.
      - Every dups_meta item references an actual survivor_event_id.

    Notes:
      - This function performs no I/O writes; it is pure orchestration over in-memory inputs.
      - Use logging to emit a one-line summary: processed, passed, rejected, groups, duplicates, conflicts.
  """
 candidates: list[dict] = []
 rejected:   list[dict] = []
 dups_meta:  list[dict] = []

 for raw_json in bronze_lines:
    try:
        payload = json.loads(raw_json)
    except JSONDecodeError:
        rejected.append({
            "reason": "malformed_json",
            "flags": [],
            "payload_raw": raw_json,
            "bronze_object_key": bronze_object_key,
            "checksum_sha256": checksum_sha256,
        })
        continue  # skip to next line

    
    row = normalize(
        payload,
        city_slug=city_slug,
        bronze_object_key=bronze_object_key,
        checksum_sha256=checksum_sha256,
    )
    if row is None:
        rejected.append({
            "reason": "missing_natural_key",
            "flags": [],
            "payload_raw": payload,  # inspect parsed payload
            "bronze_object_key": bronze_object_key,
            "checksum_sha256": checksum_sha256,
        })
        continue

    fixed, flags, verdict = validate(row)
    if verdict == "quarantine":
        rejected.append({
            "reason": "validation_failed",
            "flags": flags,
            "payload_raw": row,  # inspect normalized row
            "bronze_object_key": bronze_object_key,
            "checksum_sha256": checksum_sha256,
        })
        continue

    # only reached if not quarantined
    candidates.append(fixed)
 survivors, dups_meta = deduplicate(candidates, policy=dedup_policy)
 return survivors, dups_meta, rejected


def compute_fingerprint(row: dict) -> str: #To detect conflicting measurements in duplicated records, hash the analytical fields (provenance excluding) and act based on policies
 FINGERPRINT_FIELDS = (
    # Temperatures
    "temp_c",
    "feels_like_c",
    "temp_min_c",
    "temp_max_c",

    # Atmosphere
    "pressure_hpa",
    "humidity_pct",

    # Wind
    "wind_speed_ms",
    "wind_deg",
    "wind_gust_ms",

    # Sky / visibility
    "clouds_pct",
    "visibility_m",

    # Precipitation
    "rain_1h_mm",
    "snow_1h_mm",

    # Weather descriptors
    "weather_code",
    "weather_main",
    "weather_desc",

    # Geo
    "lat",
    "lon",
    "timezone_offset_s",
)
 content = {f : row.get(f) for f in FINGERPRINT_FIELDS}#dictionary comprehension to retrieve all analytical fields (provenance excusive)
 blob = json.dumps(content, sort_keys=True, separators=(",",":")) #Lays it all out as one single line, seperators are changed so that there is no spaces, its as compact as it gets
 return hashlib.sha256(blob.encode("utf-8")).hexdigest()
 
def policy_key(row: dict): #Function takes in a row, retuns tuple of values that define how to rank that row against other duplicates
  return ( 
    row["ingested_at_utc"],
    row["checksum_sha256"],
    row["bronze_object_key"],
    row["event_id"],
  )


def deduplicate(rows, *, policy="last_write_wins", fingerprint_fields=None) -> tuple[list[dict], list[dict]]:
 """
 Enforce uniqueness on (city_id, event_ts_utc) and mark conflicts.

 Args:
  rows (list[dict]): Validated rows with consistent primary keys.
  policy (str): Conflict resolution strategy:
                - "last": keep the lexicographically/latest row by a stable
                          tiebreaker (e.g., bronze_object_key or ingestion time).
                - "first": keep the earliest row by the same tiebreaker.

 Returns:
  list[dict]: Deduplicated rows. For any PK with >1 candidates, the retained
              row has `conflict=True` if the dropped competitor(s) had any
              differing non-provenance fields; otherwise `conflict=False`.

 Determinism:
  - Deterministic given the tiebreaker and input order normalization.

 Side Effects:
  - None.

 Notes:
  - Caller must define and document the tiebreaker used (e.g., bronze key or
    manifest timestamp) and keep it consistent across runs.
 """
 survivors: list[dict] = []
 dups_meta: list[dict] = []
 groups: dict[tuple[int, datetime], list[dict]] = defaultdict(list)
 for row in rows: 
  key = (row["city_id"], row["event_ts_utc"]) #gives you a tuple of city_id and event_ts_utc, this checks for duplicates
  groups[key].append(row) #assign keys (given by variable above) and append that row

 for key, row in groups.items():
  if len(row) == 1:
   survivors.append(row[0])
   continue
  
  pairs = [(r, compute_fingerprint(r)) for r in row] #List comprehension, stored as a tuple, iterate through row, store r (original row), as well as its computed hash counterpart
  unique_fps = {fp for _, fp in pairs} #Finds 'unique' fingerprints, if a group of 3 produces all same 3 fingerprint, then there is 1 unique finger print
  survivor = max(row, key=policy_key) #Checks for last writes, key in this scenario passes each element (i.e row) through policy key, combs through the tuples in policy_rank and outputs whichever is the highest
  #Notice how policy_key gives us a list of tuples. Comparisons are made with the first correlating field (a1, b1 : a tuple, b tuple) and evaluation stops at the first true condition
  #Key tells us what values to compare, in this case the list of tuples

  if len(unique_fps) == 1:
   #Identical rows
   survivors.append(survivor)
   for r in row:
    if r is not survivor:
      dups_meta.append({
        "key": (key[0], key[1].isoformat()),
        "survivor_event_id": survivor["event_id"],
        "dropped_event_id": r["event_id"],
        "reason": "identical",
        "policy": "last_write_wins",
      })

  else:
    flags = set(filter(None, (survivor.get("quality_flags") or "").split(";")))
    flags.add("duplicate_conflict")
    survivor["quality_flags"] = ";".join(sorted(flags))
    survivor["conflict"] = True
    survivors.append(survivor)

    for r in row:
      if r is not survivor:
        dups_meta.append({
        "key": (key[0], key[1].isoformat()),
        "survivor_event_id": survivor["event_id"],
        "dropped_event_id": r["event_id"],
        "reason": "conflict",
        "policy": "last_write_wins",
        })
 return survivors, dups_meta


  

def run(city_slug: str, target_date_utc: "datetime.date") -> dict:
    """
    End-to-end orchestration for promoting Bronze → Silver for one city-date.
    """
    date_str = target_date_utc.isoformat()

    # 1) Discover Bronze keys
    try:
        bronze_object_keys = list_bronze_keys(city_slug, target_date_utc)
    except Exception as e:
        raise RuntimeError(f"Failed to list Bronze keys for {city_slug} {date_str}: {e}")

    counters = defaultdict(int)
    counters["city_slug"] = city_slug
    counters["date_utc"] = date_str
    counters["bronze_objects"] = 0
    counters["raw_records"] = 0
    counters["normalized"] = 0
    counters["quarantined"] = 0
    counters["validated_pass"] = 0
    counters["deduped"] = 0
    counters["written_rows"] = 0
    counters["partitions"] = 0

    all_survivors: List[Dict] = []
    all_dups_meta: List[Dict] = []
    all_rejected: List[Dict] = []

    # 2) For each Bronze object: read → process microbatch
    for key in sorted(bronze_object_keys):
        try:
            payloads, checksum = read_bronze_jsonl(key)  # returns list[dict], checksum
        except Exception as e:
            raise RuntimeError(f"Failed to read Bronze object '{key}': {e}")

        counters["bronze_objects"] += 1
        counters["raw_records"] += len(payloads)

        # process_microbatch expects raw JSON lines; re-serialize parsed payloads
        bronze_lines = [json.dumps(p, separators=(",", ":")) for p in payloads]

        survivors, dups_meta, rejected = process_microbatch(
            bronze_lines=bronze_lines,
            city_slug=city_slug,
            bronze_object_key=key,
            checksum_sha256=checksum,
            dedup_policy="last_write_wins",
        )

        # Track normalized count as: raw - malformed_json - missing_natural_key
        malformed = sum(1 for r in rejected if r.get("reason") == "malformed_json")
        missing_nk = sum(1 for r in rejected if r.get("reason") == "missing_natural_key")
        counters["normalized"] += (len(payloads) - malformed - missing_nk)

        all_survivors.extend(survivors)
        all_dups_meta.extend(dups_meta)
        all_rejected.extend(rejected)

    # 3) Aggregated counters post-validation/dedup
    counters["quarantined"] = len(all_rejected)
    # Passing before dedup == survivors + dropped duplicates
    counters["validated_pass"] = len(all_survivors) + len(all_dups_meta)
    counters["deduped"] = len(all_survivors)

    # 4) (Write Silver partitions) — compute partition groups & counts
    # If you already have a writer, call it here and set written_rows/partitions from its result.
    # Below we compute the counts deterministically from survivors as they would land.
    partition_groups = defaultdict(list)
    for row in all_survivors:
        # Default partitioning by date; include hour if present
        date_part = row.get("date_utc")
        hour_part = row.get("hour_utc")
        if date_part is None:
            # derive from event_ts_utc as a safety net
            evt = row.get("event_ts_utc")
            if isinstance(evt, datetime):
                date_part = evt.date().isoformat()
                hour_part = evt.hour
        part_key = (date_part, hour_part)  # ('YYYY-MM-DD', 0-23 or None)
        partition_groups[part_key].append(row)

    # Optionally: write each partition here (e.g., to Parquet) and set written_rows accordingly.
    # For now, we mirror the final counts the writer would produce:
    counters["written_rows"] = sum(len(rows) for rows in partition_groups.values())
    counters["partitions"] = len(partition_groups)

    # 5) Guardrail (optional via env var QUARANTINE_RATE_MAX)
    q_rate_max_env = os.environ.get("QUARANTINE_RATE_MAX")
    quarantine_rate_max = float(q_rate_max_env) if q_rate_max_env else None
    quarantine_rate = (counters["quarantined"] / counters["raw_records"]) if counters["raw_records"] else 0.0

    if quarantine_rate_max is not None and quarantine_rate > quarantine_rate_max:
        # To add persist quarantine payloads here if your sink
        # (e.g., write to QUARANTINE_BUCKET) before raising.
        raise RuntimeError(
            f"Guardrail: quarantine_rate {quarantine_rate:.3f} exceeded max {quarantine_rate_max:.3f} "
            f"for {city_slug} {date_str}"
        )

    report = {
        "city_slug": city_slug,
        "date_utc": date_str,
        "bronze_objects": int(counters["bronze_objects"]),
        "raw_records": int(counters["raw_records"]),
        "normalized": int(counters["normalized"]),
        "quarantined": int(counters["quarantined"]),
        "validated_pass": int(counters["validated_pass"]),
        "deduped": int(counters["deduped"]),
        "written_rows": int(counters["written_rows"]),
        "partitions": int(counters["partitions"]),
        "quarantine_rate": float(quarantine_rate),
        "policy": {"dedup": "last_write_wins", "partitioning": "date|date_hour"},
    }
    return report


report = run("london", date(2025, 9, 4))
print(report)