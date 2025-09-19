from datetime import datetime, timezone
import hashlib
import json
from json import JSONDecodeError








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
- hour_utc: int   (0–23; optional partition)

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


def normalize(payload, *, city_slug, bronze_key, checksum_sha256):
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

# --- Geo / time ---
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
    "bronze_object_key": bronze_key,
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


def process_microbatch(bronze_lines: list[str], city_slug:str , bronze_key:str , checksum_sha256:str , dedup_policy:str) -> tuple[list[dict], list[dict], list[dict]]:
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
    except json.JSONDecodeError:
        rejected.append({
            "reason": "malformed_json",
            "flags": [],
            "payload_raw": raw_json,
            "bronze_object_key": bronze_key,
            "checksum_sha256": checksum_sha256,
        })
        continue  # skip to next line

    
    row = normalize(
        payload,
        city_slug=city_slug,
        bronze_key=bronze_key,
        checksum_sha256=checksum_sha256,
    )
    if row is None:
        rejected.append({
            "reason": "missing_natural_key",
            "flags": [],
            "payload_raw": payload,  # inspect parsed payload
            "bronze_object_key": bronze_key,
            "checksum_sha256": checksum_sha256,
        })
        continue

    fixed, flags, verdict = validate(row)
    if verdict == "quarantine":
        rejected.append({
            "reason": "validation_failed",
            "flags": flags,
            "payload_raw": row,  # inspect normalized row
            "bronze_object_key": bronze_key,
            "checksum_sha256": checksum_sha256,
        })
        continue

    # only reached if not quarantined
    candidates.append(fixed)


def compute_fingerprint(row) #To detect conflicting measurements in duplicated records, hash the analytical fields (provenance excluding) and act based on policies
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
 return sha256(blob.encode("utf-8")).hexdigest()
 

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
 groups : defaultdict(list)
 for row in rows: 
  key = (row["city_id"], row["event_ts_utc"]) #gives you a tuple of city_id and event_ts_utc, this checks for duplicates
  groups[key].append(row) #assign keys (given by variable above) and append that row

 for key, item in groups.items():
#To finish
  

"""""
End-to-end orchestration for promoting Bronze → Silver for one city-date.

Args:
  city_slug (str): Canonical city slug (e.g., "london").
  target_date_utc (datetime.date): UTC calendar date to process.

Returns:
  dict: Pipeline report:
        {
          "city_slug": str,
          "date_utc": "YYYY-MM-DD",
          "bronze_objects": int,
          "raw_records": int,
          "normalized": int,
          "quarantined": int,
          "validated_pass": int,
          "deduped": int,
          "written_rows": int,
          "partitions": int,
          "quarantine_rate": float,  # 0.0-1.0
          "policy": { "dedup": "last", "partitioning": "date|date_hour" }
        }

Raises:
  RuntimeError: On unrecoverable I/O or write failures.

Flow:
  1) Discover Bronze object keys for (city_slug, date_utc).
  2) For each key:
       - Read JSONL → payloads, compute checksum.
       - Normalize each payload to a row (or quarantine if missing keys).
       - Validate each row → pass or quarantine.
  3) Deduplicate passing rows by (city_id, event_ts_utc) using policy.
  4) Write Silver partitions; collect paths and counts.
  5) Compute and return the report (and optionally enforce a guardrail:
     fail if quarantine_rate > threshold).

Idempotency:
  - Safe to re-run for the same city/date; results should converge.

Guardrails:
  - Optionally raise if quarantine_rate exceeds configured threshold.
"""

"""
CLI entrypoint for manual or scheduled runs.

Behavior:
  - Parse environment or CLI args for `city_slug` and `target_date_utc`
    (default: today's UTC date).
  - Call `run(...)` and print a concise report.
  - Exit non-zero on guardrail violations or write failures.

Notes:
  - Keep orchestration in `run`; `main` should remain a thin wrapper.
"""