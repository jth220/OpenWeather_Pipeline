import requests
import os
from dotenv import load_dotenv
load_dotenv("settings.env")
import time
import re
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import logging
from datetime import datetime, timezone
import hashlib
import boto3
import json
from botocore.exceptions import ClientError, BotoCoreError
from uuid import uuid4

logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more verbosity
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ingest.openweather")

OWM_API_KEY = os.environ["OWM_API_KEY"]
CITY_NAME = os.environ.get("CITY_NAME", "London")
MAX_PAYLOAD_BYTES = int(os.environ.get("MAX_PAYLOAD_BYTES","5242880"))
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BRONZE_BUCKET= os.environ.get("BRONZE_BUCKET")
QUARANTINE_BUCKET=os.environ.get("QUARANTINE_BUCKET")

client = boto3.client('s3', 
                      aws_access_key_id = AWS_ACCESS_KEY_ID,
                      aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                      region_name = "eu-north-1",
                      )

def redact_url(url: str) -> str:
    """
    Redacts sensitive query parameters (like API keys) from a URL.
    """
    parsed = urlparse(url)
    query_pairs = dict(parse_qsl(parsed.query))

    if "appid" in query_pairs:
        query_pairs["appid"] = "***"

    redacted_query = urlencode(query_pairs)
    return urlunparse(parsed._replace(query=redacted_query))

def slug(city):
  """Convert a city name into a lowercase, dash-safe slug for storage paths."""
  return re.sub('[^0-9a-zA-Z]+', '-', (city.lower())).strip('-')

def fetch_openweather(city: str, api_key: str, timeout_sec: tuple) -> tuple[dict, str, str, float] | None :
    '''
     Purpose:
      Safely fetch OpenWeather current weather for a given city. On success, return the raw JSON payload and metadata.
      On failure, quarantine the issue and return None.

     Args:
      city: <what format you expect, e.g., human-readable name like "London">
      api_key: <where it comes from, e.g., env var OWM_API_KEY passed in>
      timeout_sec: (connect_timeout, read_timeout)

     Returns:
      A tuple (payload, base_url, full_url, duration_sec) on success
      or None if quarantined.

     Side effects:
      - May call quarantine(reason, raw_text, city_slug) on errors.
      - Logs retry attempts and outcomes.

     Success conditions:
      - HTTP 200
      - response size <= MAX_PAYLOAD_BYTES
      - body parses as JSON

     Retry policy:
      - Retry on network/timeout, 5xx, and 429 (respect Retry-After)
      - Max attempts: 5, exponential backoff starting at 2s, capped at 60s

     Quarantine conditions:
      - client_error:<status> for 4xx (except 429)
      - payload_too_large if body > MAX_PAYLOAD_BYTES
      - malformed_json if JSON parsing fails
      - retry_exhausted:<error> if final attempt fails
     '''
    headers = { #This is so the server can identify who is sending the requests
      "Accept" : 'application/json',
      "User-Agent": "hello-cloud-ingest/1.0"
    }
    
    params = { "q": city, 
               "appid": api_key, 
               "units": "metric" 
             }
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    last_err = ""
    

    
    for attempt in range(1, 6): #Counts attempt
      start_timer = time.time()
      base_delay = 2
      backoff_delay = min(base_delay * (2 ** (attempt - 1)), 60) #Calculates delay, 2, 4, ...caps upto 60 seconds
      try:
       response = requests.get(base_url, params=params, headers=headers, timeout=timeout_sec)
       duration = time.time() - start_timer
       logger.info(
       "attempt=%d city=%s status=%d duration_ms=%d result=ok",
       attempt, slug(city), response.status_code, int(duration * 1000)
       )
       if response.status_code == 200: #Succesful connection
        if len(response.content) > MAX_PAYLOAD_BYTES: #Checks for valid size
          quarantine("payload_too_large", response.text, slug(city))
          last_err = (f"Payload too large")
          logger.warning(
          "attempt=%d city=%s status=%d duration_ms=%d result=quarantine reason=payload_too_large",
          attempt, slug(city), response.status_code, int(duration * 1000)
          )
          return None

        else:
          try:
           payload = response.json()
           return (payload, base_url, redact_url(response.url), duration)
          except ValueError as e:
            quarantine("malformed_json", response.text, slug(city))
            last_err = (str(e))
            logger.warning(
            "attempt=%d city=%s status=%d duration_ms=%d result=quarantine reason=malformed_json",
            attempt, slug(city), response.status_code, int(duration * 1000)
            )
            return None
            
       elif response.status_code >= 400 and response.status_code != 429: #Client error
          quarantine(f"client_error:{response.status_code}", response.text, slug(city))
          last_err = (f"client_error:{response.status_code}")
          logger.error(
          "attempt=%d city=%s status=%d duration_ms=%d result=quarantine reason=client_error:%d",
          attempt, slug(city), response.status_code, int(duration * 1000), response.status_code
          )
          return None


       elif response.status_code == 429: #Too many requests, use the Retry-After header from the reponse to decide how long to sleep the execution
         header_response = response.headers.get("Retry-After")
         if header_response and header_response.isdigit():
           delay = int(header_response)
           source = "retry-after"
         else:
           delay = backoff_delay
           source = "backoff-delay"
         last_err = (f"{response.status_code}")
         logger.info(
         "attempt=%d city=%s status=%d duration_ms=%d action=retry next_delay_s=%d source=%s",
         attempt, slug(city), response.status_code, int(duration * 1000), delay, source
         )
         time.sleep(delay)
         continue
          

       elif response.status_code >= 500:
         logger.info(
         "attempt=%d city=%s status=%d duration_ms=%d action=retry next_delay_s=%d",
         attempt, slug(city), response.status_code, int(duration * 1000), backoff_delay
         )
         time.sleep(backoff_delay)
         continue
       
    
      except requests.exceptions.Timeout as e:
        last_err = str(e)
        print("Request Timed out")
        logger.error(
        "attempt=%d city=%s error=%s duration_ms=%d action=retry next_delay_s=%d",
        attempt, slug(city), str(e), int((time.time() - start_timer) * 1000), backoff_delay
        )

        time.sleep(backoff_delay)
        continue

      except requests.exceptions.RequestException as e:
        last_err = str(e)
        logger.error(
        "attempt=%d city=%s error=%s duration_ms=%d action=retry next_delay_s=%d",
        attempt, slug(city), str(e), int((time.time() - start_timer) * 1000), backoff_delay
        )
        time.sleep(backoff_delay)
        continue
    
    quarantine(f"retry_exhausted:{last_err}", "", slug(city))
    logger.critical(
    "attempt=max city=%s result=quarantine reason=retry_exhausted:%s",
    slug(city), last_err
    )
    return None

def quarantine(reason, raw_text, city_slug):
  return None

def derive_event_id(city_slug, payload_dict) -> tuple[str, datetime]:
  """
    Purpose:
      Derive a deterministic identifier and event timestamp for a single
      OpenWeather API response. This ensures idempotency: the same natural
      key always produces the same event ID and partition time.

    Args:
      city_slug: Canonical slug for the city (e.g., "london").
      payload_dict: Parsed JSON payload from OpenWeather. Must include
                    "id" (city ID) and "dt" (UTC seconds since epoch).

    Returns:
      (event_id_hex24, event_ts_utc)
        event_id_hex24: 24-character hex string derived from hashing
                        f"owm:{city_id}:{dt}".
        event_ts_utc:   A timezone-aware datetime in UTC corresponding to
                        payload["dt"].

    Success conditions:
      - payload contains valid "id" (int) and "dt" (int).
      - event_ts_utc is within a plausible range (e.g., after 2000-01-01).

    Failure conditions (should trigger quarantine):
      - Missing or invalid natural key fields.
      - Non-numeric or nonsensical "dt".

    Side effects:
      None (pure function).

    Determinism:
      Same input → same event ID + timestamp.
    """
  

  try:
   id, dt = int(payload_dict.get("id")), int(payload_dict.get("dt"))
  
  except (ValueError, TypeError, KeyError):
    quarantine("missing_natural_key", str(payload_dict), city_slug)
    return None
  

  event_ts_utc = datetime.fromtimestamp(dt, timezone.utc) #Converts unix timestamp to a datetime object in UTC
  
  #Build deterministic event ID
  raw_key = (f"owm:{id}:{dt}") #
  event_id_hex24 = hashlib.sha1(raw_key.encode()).hexdigest()[:24] #Look into this later
  return (event_id_hex24, event_ts_utc)


  
    
def bronze_prefix(city_slug, event_ts_utc) -> str:
    """
    Purpose:
      Build the Bronze storage prefix (S3/MinIO key path) for a weather event,
      partitioned by city and event time in UTC. This provides stable,
      query-friendly organization of raw events.

    Args:
      city_slug: Canonical slug for the city (e.g., "london").
      event_ts_utc: Event timestamp as a timezone-aware UTC datetime.

    Returns:
      A string prefix of the form:
        "weather/city=<slug>/date=YYYY-MM-DD/hour=HH/"
      Example:
        "weather/city=london/date=2025-09-03/hour=15/"

    Success conditions:
      - event_ts_utc is a valid datetime with tzinfo=UTC.

    Failure conditions:
      - event_ts_utc missing tzinfo or not UTC (should raise or sanitize).

    Side effects:
      None (pure function).

    Determinism:
      Same slug + same timestamp hour → same prefix path.
    """
    if not isinstance(event_ts_utc, datetime):
     raise TypeError("event_ts_utc must be a datetime")
    
    if event_ts_utc.tzinfo != timezone.utc:
     raise ValueError("event_ts_utc must be timezone-aware and in UTC")
     
    event_ts_utc_date = event_ts_utc.strftime('%Y-%m-%d')
    event_ts_utc_hour = event_ts_utc.strftime('%H')

    format_prefix = f"weather/city={city_slug}/date={event_ts_utc_date}/hour={event_ts_utc_hour}/"
    return format_prefix






def write_bronze_line(bucket, key, line_bytes):
 """
    Purpose:
      Atomically write a single JSON Lines (JSONL) record to the Bronze layer at the
      exact object key provided. This is the immutable raw event artifact.

    Args:
      bucket:
        Name of the object storage bucket (e.g., "bronze").
      key:
        Full object key including prefix and filename. Typically built from:
          bronze_prefix(city_slug, event_ts_utc) + f"{city_slug}-{event_id}.jsonl"
        Example:
          "weather/city=london/date=2025-09-03/hour=15/london-5d3f...e91a.jsonl"
      line_bytes:
        UTF-8 encoded JSON line (exactly one record) **including a trailing newline**.
        Caller is responsible for encoding and ensuring one-line JSON.

    Returns:
      (bytes_written, etag)
        bytes_written: Number of bytes actually written (int).
        etag: Storage ETag or content hash returned by the store (implementation-specific).

    Success conditions:
      - Object PUT succeeds in the target bucket/key.
      - Written size equals len(line_bytes).

    Failure conditions (raise or bubble up):
      - Storage client errors (e.g., permission, missing bucket).
      - Transient network errors (caller may choose to retry).
      - Non-idempotent conflicts if key already exists and store enforces uniqueness
        (define semantics below).

    Side effects:
      - Creates or overwrites the object at {bucket}/{key}. Choose one policy:
          • Idempotent overwrite: safe because same event_id → same content.
          • Conditional create: use "if-none-match: *" to avoid accidental dupes.

    Determinism:
      - Key must be derived from deterministic inputs (city_slug, event_ts_utc hour, event_id).
      - Content is the exact raw payload (verbatim) optionally with minimal provenance added
        by the caller before encoding.

    Notes:
      - Set Content-Type to "application/x-ndjson" or "application/json".
      - Consider server-side encryption and storage class if available.
      - This function should NOT mutate or reformat payloads—Bronze is immutable raw.
    """
 
 #validating Inputs
 if not isinstance(bucket, str) or not bucket:
   logger.error("bronze_write invalid bucket=%r", bucket)
   return None

 if not isinstance(key, str) or not key:
   logger.error("bronze_write invalid key=%r", key)
   return None
 
 if not isinstance(line_bytes, (bytes, bytearray)):
        logger.error("bronze_write line_bytes must be bytes, got %s", type(line_bytes).__name__)
        return None
   
 if not line_bytes.endswith(b"\n"):
  line_bytes +=(b"\n")


 try:
        resp = client.put_object(
            Bucket=bucket,
            Key=key,
            Body=line_bytes,
            ContentType="application/x-ndjson"
        )
        etag = resp.get("ETag", "").strip('"')
        bytes_written = len(line_bytes)
        logger.info(
            "bronze_write bucket=%s key=%s bytes=%d etag=%s result=ok",
            bucket, key, bytes_written, etag
        )
        return (bytes_written, etag)

 except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        msg = e.response.get("Error", {}).get("Message")
        logger.error(
            "bronze_write bucket=%s key=%s result=error code=%s msg=%s",
            bucket, key, code, msg
        )
        return None
 except BotoCoreError as e:
        logger.error(
            "bronze_write bucket=%s key=%s result=error botocore=%s",
            bucket, key, str(e)
        )
        return None

def write_manifest(
    bucket: str,
    prefix: str,
    event_id: str,
    run_id: str,
    source_url_redacted: str,
    record_count: int,
    checksum_hex: str,
    bytes_written: int,
    fetch_duration_sec: float | None = None,
    attempt_count: int | None = None,
) -> tuple[int, str]:
    """
    Purpose:
      Persist a small JSON manifest alongside the Bronze object to capture provenance,
      lineage, and write metadata. This makes debugging and audits trivial.

    Args:
      bucket:
        Name of the object storage bucket (e.g., "bronze").
      prefix:
        The Bronze partition prefix for this event, produced by bronze_prefix(...).
        Example: "weather/city=london/date=2025-09-03/hour=15/"
      event_id:
        24-hex deterministic ID for the event (e.g., from derive_event_id()).
      run_id:
        Unique identifier for this pipeline run (e.g., UUID4).
      source_url_redacted:
        The request URL with secrets removed (use redact_url()).
      record_count:
        Number of JSONL records written for this event (usually 1).
      checksum_hex:
        Hex digest (e.g., SHA-256) of the exact bytes written to the Bronze object.
        Enables integrity checks and idempotent verification.
      bytes_written:
        Exact byte length of the Bronze object written.
      fetch_duration_sec:
        Optional: Total fetch latency (from the ingestion function), for observability.
      attempt_count:
        Optional: Number of attempts taken to fetch (retries included).

    Returns:
      (bytes_written, etag)
        bytes_written: Number of bytes written for the manifest file.
        etag: Storage ETag for the manifest object.

    Key naming:
      - The manifest sits next to the data file under the same prefix:
          {prefix}manifest-{event_id}.json
        Example:
          "weather/city=london/date=2025-09-03/hour=15/manifest-5d3f...e91a.json"

    Manifest JSON schema (example fields):
      {
        "version": 1,
        "run_id": "<uuid4>",
        "event_id": "<24hex>",
        "city_partition": "<city_slug>",
        "prefix": "<same as input>",
        "object_key": "<prefix + filename>",
        "record_count": 1,
        "bytes_written": 1234,
        "checksum_sha256": "<hex>",
        "fetched_at_utc": "<ISO-8601>",
        "fetch_duration_sec": 0.183,
        "attempt_count": 2,
        "source": {
          "provider": "openweather",
          "endpoint": "current",
          "url_redacted": "<...>"
        }
      }

    Success conditions:
      - Manifest is written successfully and references an existing Bronze object.

    Failure conditions (raise or bubble up):
      - Storage client errors.
      - Serialization failures (should not happen if inputs are simple types).

    Side effects:
      - Creates or overwrites the manifest at {prefix}/manifest-{event_id}.json.

    Determinism:
      - Manifest filename determined solely by event_id; safe to overwrite if the same
        event is written again with identical content.

    Notes:
      - Keep the manifest small (< 10 KB).
      - Never store secrets; always use redacted URLs.
      - Include enough context to re-trace: run_id, source, timing, integrity hash.
    """
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    manifest_key = f"{prefix}manifest-{event_id}.json"


    manifest = {
      "version": 1,
        "run_id": run_id,
        "event_id": event_id,
        "prefix": prefix,
        "bucket" : bucket,
        "record_count": record_count,
        "bytes_written": bytes_written,
        "fetch_duration_sec": fetch_duration_sec,
        "attempt_count": attempt_count,
        "source": {
          "provider": "openweather",
          "endpoint": "current",
          "url_redacted": source_url_redacted

         }
    }
    
    body_bytes = (json.dumps(manifest, separators=(",",":")))
    try:
      resp = client.put_object(
      Bucket = bucket,
      Key = manifest_key,
      Body = body_bytes
      )

      m_etag = resp.get("ETag", "").strip('"')
      m_bytes = len(body_bytes)
      logger.info(
            "manifest_write bucket=%s key=%s bytes=%d etag=%s result=ok",
            bucket, manifest_key, m_bytes, m_etag
      )
      return (m_bytes, m_etag)
    except (ClientError, BotoCoreError) as e:
        logger.error(
            "manifest_write bucket=%s key=%s error=%s result=error",
            bucket, manifest_key, str(e)
        )
        return None

    


  






def main():
    city_slug = slug(CITY_NAME)
    run_id = uuid4().hex

    fetch = fetch_openweather(CITY_NAME, OWM_API_KEY, (5, 5))
    if fetch is None:
        logger.error("main result=exit reason=fetch_failed city=%s", city_slug)
        return

    payload, base_url, url_redacted, duration = fetch

    derived = derive_event_id(city_slug, payload)
    if derived is None:
        logger.error("main result=exit reason=derive_failed city=%s", city_slug)
        return
    event_id, event_ts_utc = derived

    prefix = bronze_prefix(city_slug, event_ts_utc)
    data_key = f"{prefix}{city_slug}-{event_id}.jsonl"

    json_str = json.dumps(payload, separators=(",", ":"), sort_keys=True) #json.dumps formats a json structure into one line, seperators are used to remove white sapces after , and :
    line_bytes = json_str.encode("utf-8") + b"\n" #Encodes entire string, marks the end of a record as byte literal new line

    wrote = write_bronze_line(BRONZE_BUCKET, data_key, line_bytes)
    if wrote is None:
        logger.error("main result=exit reason=bronze_write_failed key=%s", data_key)
        return
    bytes_written, etag = wrote

    sha256_hex = hashlib.sha256(line_bytes).hexdigest()

    _ = write_manifest(
        bucket=BRONZE_BUCKET,
        prefix=prefix,  # <- use the variable you created above (note: case sensitive)
        event_id=event_id,
        run_id=run_id,
        source_url_redacted=url_redacted,
        record_count=1,
        checksum_hex=sha256_hex,
        bytes_written=bytes_written,
        fetch_duration_sec=duration,
        attempt_count=None,
    )

    logger.info(
        "main result=ok city=%s event_id=%s key=%s bytes=%d etag=%s",
        city_slug, event_id, data_key, bytes_written, etag
    )



if __name__ == "__main__":
    main()