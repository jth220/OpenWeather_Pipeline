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

logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more verbosity
    format="%(asctime)s %(rlevelname)s %(message)s",
)
logger = logging.getLogger("ingest.openweather")

OWM_API_KEY = os.environ["OWM_API_KEY"]
CITY_NAME = os.environ.get("CITY_NAME", "London")
MAX_PAYLOAD_BYTES = int(os.environ.get("MAX_PAYLOAD_BYTES","5242880"))



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
         "attempt=%d city=%s status=%d duration_ms=%d action=retry next_delay_s=%d source=%d",
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
  

  event_ts_utc = datetime.fromtimestamp(dt, tz=timezone.utc) #Converts unix timestamp to a datetime object in UTC
  
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





'''
def write_bronze_line(bucket, key, line_bytes):


def write_manifest(bucket, prefix, event_id, run_id, source_url, record_count, checksum, bytes_written):





def main():

'''


print(fetch_openweather(CITY_NAME, OWM_API_KEY, (5, 5)))
print("Hello")