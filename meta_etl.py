import os
import re
import sys
import json
import time
import uuid
import ssl
import smtplib
from email.message import EmailMessage
from datetime import date, timedelta, datetime

import requests
from dotenv import load_dotenv

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# =========================
# Env & constants
# =========================
load_dotenv()

EMAI_PASSWORD = os.getenv("EMAI_PASSWORD")  # (yes, "EMAI" to match your existing)
SENDER_EMAIL = "monitoring@brightfire.net"
RECEIVER_EMAIL = "danylo@brightfire.net"

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_API_VER = os.getenv("META_API_VER", "v22.0")
META_ATTRIBUTION_SETTING = os.getenv("META_ATTRIBUTION_SETTING")  # optional, e.g. "7d_click"
AD_ACCOUNTS = [s.strip() for s in os.getenv("META_AD_ACCOUNTS", "").split(",") if s.strip()]

# Snowflake
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "SANDBOX")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DANYLO")

# Pathing
OUT_DIR = "/home/service_account/meta_ads_etl/temp"
os.makedirs(OUT_DIR, exist_ok=True)

# Files we'll produce (to mirror your Google Ads names)
OUT_CAMPAIGN_DAY = os.path.join(OUT_DIR, "campaign_by_day.json")
OUT_CAMPAIGN_HOUR = os.path.join(OUT_DIR, "campaign_by_hour.json")
OUT_ADSET_PLATFORM = os.path.join(OUT_DIR, "adset_platform_by_day.json")
OUT_ADSET_DEMOGRAPHICS = os.path.join(OUT_DIR, "adset_demographics_by_day.json")
OUT_CAMPAIGN_METADATA = os.path.join(OUT_DIR, "campaign_metadata.json")

# =========================
# Utilities
# =========================
def send_email(subject: str, body: str):
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECEIVER_EMAIL

        context = ssl.create_default_context()
        with smtplib.SMTP("smtp.office365.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.login(SENDER_EMAIL, EMAI_PASSWORD)
            smtp.send_message(msg)
    except Exception as e:
        print(f"[WARN] Failed to send email: {e}")

def backoff_sleep(attempt: int):
    # simple exponential backoff with jitter
    base = min(60, (2 ** attempt))
    time.sleep(base + (uuid.uuid4().int % 1000) / 1000.0)

def _flatten_record(rec: dict) -> dict:
    """
    Keep keys flat, uppercase block-style similar to your GA flow.
    We’ll prefix breakdowns and core metrics consistently.
    """
    out = {}
    # Dates - use just the start date for daily data
    out["SEGMENTS_DATE"] = rec.get("date_start")

    # IDs
    if "account_id" in rec: out["ACCOUNT_ID"] = rec["account_id"]
    if "campaign_id" in rec: out["CAMPAIGN_ID"] = rec["campaign_id"]
    if "adset_id" in rec: out["ADSET_ID"] = rec["adset_id"]
    if "ad_id" in rec: out["AD_ID"] = rec["ad_id"]

    # Currency & attribution
    if "account_currency" in rec: out["CUSTOMER_CURRENCYCODE"] = rec["account_currency"]
    if "attribution_setting" in rec: out["ATTRIBUTION_SETTING"] = rec["attribution_setting"]

    # Delivery metrics
    for k in ("spend", "impressions", "clicks", "cpc", "cpm", "ctr",
              "reach", "frequency", "unique_clicks"):
        if k in rec:
            out[f"METRICS_{k.upper()}"] = rec[k]
    
    # Conversion metrics
    for k in ("actions",):
        if k in rec:
            out[f"METRICS_{k.upper()}"] = rec[k]

    # Breakdowns we might request
    for b in ("publisher_platform", "device_platform", "age", "gender", "hour"):
        if b in rec:
            out[f"SEGMENTS_{b.upper()}"] = rec[b]

    # API + watermark
    if "api_version" in rec: out["API_VERSION"] = rec["api_version"]
    out["_RIVERY_LAST_UPDATE"] = datetime.utcnow().isoformat()

    # Remove None values (Snowflake copy will treat absent keys fine)
    return {k: v for k, v in out.items() if v is not None}

# =========================
# Meta API
# =========================
def fetch_insights(ad_account: str, params: dict):
    """
    Generator that pages through the /insights endpoint.
    """
    assert META_ACCESS_TOKEN, "META_ACCESS_TOKEN not set"
    base = f"https://graph.facebook.com/{META_API_VER}/{ad_account}/insights"
    params = dict(params)
    params["access_token"] = META_ACCESS_TOKEN



    url = base
    attempt = 0
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    while True:
        try:
            r = session.get(url, params=params if url == base else None, timeout=120)
            if r.status_code in (429, 400, 403):
                attempt += 1
                print(f"[THROTTLE] {r.status_code}. Backing off (attempt {attempt})...")
                if r.status_code == 400:
                    print(f"[DEBUG] 400 Error response: {r.text}")
                backoff_sleep(attempt)
                continue
            r.raise_for_status()
            data = r.json()
            
        except Exception as e:
            attempt += 1
            if attempt > 5:
                raise
            print(f"[WARN] fetch_insights error: {e}; retrying ({attempt})")
            backoff_sleep(attempt)
            continue

        for row in data.get("data", []):
            yield row

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url = next_url

def fetch_campaigns(ad_account: str):
    """
    Fetch campaign metadata (ID, name, status, etc.) for an ad account.
    """
    assert META_ACCESS_TOKEN, "META_ACCESS_TOKEN not set"
    base = f"https://graph.facebook.com/{META_API_VER}/{ad_account}/campaigns"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "id,name,status,objective,created_time,updated_time,start_time,stop_time,special_ad_categories,special_ad_category,special_ad_category_country"
    }



    url = base
    attempt = 0
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    while True:
        try:
            r = session.get(url, params=params if url == base else None, timeout=120)
            if r.status_code in (429, 400, 403):
                attempt += 1
                print(f"[THROTTLE] {r.status_code}. Backing off (attempt {attempt})...")
                if r.status_code == 400:
                    print(f"[DEBUG] 400 Error response: {r.text}")
                backoff_sleep(attempt)
                continue
            r.raise_for_status()
            data = r.json()
            
        except Exception as e:
            attempt += 1
            if attempt > 5:
                raise
            print(f"[WARN] fetch_campaigns error: {e}; retrying ({attempt})")
            backoff_sleep(attempt)
            continue

        for row in data.get("data", []):
            # Add account_id and api_version to each campaign record
            row["account_id"] = ad_account
            row["api_version"] = META_API_VER
            yield row

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url = next_url

def write_ndjson(records, path):
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec))
            f.write("\n")

# =========================
# “Blocks” (to mirror your style)
# =========================
def block_campaign_by_day(since: date, until: date):
    fields = [
        "date_start","date_stop",
        "account_id","campaign_id",
        "spend","impressions","clicks","cpc","cpm","ctr",
        "reach","frequency","unique_clicks",
        "actions",
        "attribution_setting","account_currency"
    ]
    params = {
        "level": "campaign",
        "time_increment": 1,  # daily
        "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
        "fields": ",".join(fields),
    }
    if META_ATTRIBUTION_SETTING:
        params["attribution_setting"] = META_ATTRIBUTION_SETTING

    out_rows = []
    for act in AD_ACCOUNTS:
        print(f"[DEBUG] Processing account: {act}")
        record_count = 0
        for raw in fetch_insights(act, params):
            raw["api_version"] = META_API_VER
            out_rows.append(_flatten_record(raw))
            record_count += 1
        print(f"[DEBUG] Account {act}: {record_count} records")
    write_ndjson(out_rows, OUT_CAMPAIGN_DAY)
    print(f"[OK] Wrote {len(out_rows)} rows → {OUT_CAMPAIGN_DAY}")

def block_campaign_by_hour(since: date, until: date):
    """
    Hourly breakdown using 'hourly_stats_aggregated_by_advertiser_time_zone'.
    We’ll add “hour” in flattened output as SEGMENTS_HOUR (00..23).
    """
    fields = [
        "date_start","date_stop",
        "account_id","campaign_id",
        "spend","impressions","clicks","cpc","cpm","ctr",
        "actions",
        "attribution_setting","account_currency"
    ]
    params = {
        "level": "campaign",
        "time_increment": 1,  # still daily, but we add the hourly breakdown
        "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
        "fields": ",".join(fields),
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone"  # Use advertiser timezone hour breakdown
    }
    if META_ATTRIBUTION_SETTING:
        params["attribution_setting"] = META_ATTRIBUTION_SETTING

    out_rows = []
    for act in AD_ACCOUNTS:
        print(f"[DEBUG] Processing account: {act}")
        record_count = 0
        for raw in fetch_insights(act, params):
            # Transform: API returns "hourly_stats_aggregated_by_advertiser_time_zone":"15:00:00"
            if "hourly_stats_aggregated_by_advertiser_time_zone" in raw:
                raw["hour"] = raw["hourly_stats_aggregated_by_advertiser_time_zone"][:2]
            raw["api_version"] = META_API_VER
            out_rows.append(_flatten_record(raw))
            record_count += 1
        print(f"[DEBUG] Account {act}: {record_count} records")
    write_ndjson(out_rows, OUT_CAMPAIGN_HOUR)
    print(f"[OK] Wrote {len(out_rows)} rows → {OUT_CAMPAIGN_HOUR}")

def block_adset_platform_by_day(since: date, until: date):
    fields = [
        "date_start","date_stop",
        "account_id","campaign_id","adset_id",
        "spend","impressions","clicks","cpc","cpm","ctr",
        "reach","frequency","unique_clicks",
        "actions",
        "attribution_setting","account_currency"
    ]
    
    params = {
        "level": "adset",
        "time_increment": 1,
        "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
        "fields": ",".join(fields),
        "breakdowns": "publisher_platform,device_platform"
    }
    if META_ATTRIBUTION_SETTING:
        params["attribution_setting"] = META_ATTRIBUTION_SETTING

    out_rows = []
    for act in AD_ACCOUNTS:
        print(f"[DEBUG] Processing account {act} - platform breakdowns")
        record_count = 0
        for raw in fetch_insights(act, params):
            raw["api_version"] = META_API_VER
            out_rows.append(_flatten_record(raw))
            record_count += 1
        print(f"[DEBUG] Account {act} platform breakdowns: {record_count} records")
    
    write_ndjson(out_rows, OUT_ADSET_PLATFORM)
    print(f"[OK] Wrote {len(out_rows)} rows → {OUT_ADSET_PLATFORM}")

def block_adset_demographics_by_day(since: date, until: date):
    fields = [
        "date_start","date_stop",
        "account_id","campaign_id","adset_id",
        "spend","impressions","clicks","cpc","cpm","ctr",
        "reach","frequency","unique_clicks",
        "actions",
        "attribution_setting","account_currency"
    ]
    
    params = {
        "level": "adset",
        "time_increment": 1,
        "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
        "fields": ",".join(fields),
        "breakdowns": "age,gender"
    }
    if META_ATTRIBUTION_SETTING:
        params["attribution_setting"] = META_ATTRIBUTION_SETTING

    out_rows = []
    for act in AD_ACCOUNTS:
        print(f"[DEBUG] Processing account {act} - demographic breakdowns")
        record_count = 0
        for raw in fetch_insights(act, params):
            raw["api_version"] = META_API_VER
            out_rows.append(_flatten_record(raw))
            record_count += 1
        print(f"[DEBUG] Account {act} demographic breakdowns: {record_count} records")
    
    write_ndjson(out_rows, OUT_ADSET_DEMOGRAPHICS)
    print(f"[OK] Wrote {len(out_rows)} rows → {OUT_ADSET_DEMOGRAPHICS}")

def block_campaign_metadata():
    """
    Fetch campaign metadata (names, status, etc.) for all ad accounts.
    This doesn't need date parameters since we want all campaigns.
    """
    out_rows = []
    for act in AD_ACCOUNTS:
        record_count = 0
        try:
            for raw in fetch_campaigns(act):
                # Flatten the campaign record
                flattened = _flatten_campaign_record(raw)
                out_rows.append(flattened)
                record_count += 1
        except Exception as e:
            print(f"[ERROR] Failed to fetch campaigns for account {act}: {e}")
            import traceback
            traceback.print_exc()
    
    write_ndjson(out_rows, OUT_CAMPAIGN_METADATA)
    print(f"[OK] Wrote {len(out_rows)} campaign metadata rows → {OUT_CAMPAIGN_METADATA}")

def _flatten_campaign_record(rec: dict) -> dict:
    """
    Flatten campaign metadata record for consistent formatting.
    """
    out = {}
    
    # Core campaign fields
    if "id" in rec: out["CAMPAIGN_ID"] = rec["id"]
    if "name" in rec: out["CAMPAIGN_NAME"] = rec["name"]
    if "status" in rec: out["CAMPAIGN_STATUS"] = rec["status"]
    if "objective" in rec: out["CAMPAIGN_OBJECTIVE"] = rec["objective"]
    
    # Timestamps - convert to UTC format that Snowflake can handle
    def convert_timestamp(ts_str):
        if not ts_str:
            return None
        try:
            # Parse the ISO timestamp and convert to UTC format
            from datetime import datetime
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    if "created_time" in rec: out["CREATED_TIME"] = convert_timestamp(rec["created_time"])
    if "updated_time" in rec: out["UPDATED_TIME"] = convert_timestamp(rec["updated_time"])
    if "start_time" in rec: out["START_TIME"] = convert_timestamp(rec["start_time"])
    if "stop_time" in rec: out["STOP_TIME"] = convert_timestamp(rec["stop_time"])
    
    # Special ad categories
    if "special_ad_categories" in rec: out["SPECIAL_AD_CATEGORIES"] = rec["special_ad_categories"]
    if "special_ad_category" in rec: out["SPECIAL_AD_CATEGORY"] = rec["special_ad_category"]
    if "special_ad_category_country" in rec: out["SPECIAL_AD_CATEGORY_COUNTRY"] = rec["special_ad_category_country"]
    
    # Account and API info
    if "account_id" in rec: out["ACCOUNT_ID"] = rec["account_id"]
    if "api_version" in rec: out["API_VERSION"] = rec["api_version"]
    
    # Watermark - use UTC format
    out["_RIVERY_LAST_UPDATE"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Remove None values
    return {k: v for k, v in out.items() if v is not None}

# =========================
# Snowflake loading (PUT → COPY → MERGE)
# =========================
def open_snowflake_connection():
    # If you prefer key-pair auth, adapt to your rsa key like your google script.
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def load_file_to_table(cs, file_path: str, table: str, key_columns: list):
    """
    Matches your Google uploader logic:
    - PUT to @~
    - COPY into temp staging (match_by_column_name case-insensitive)
    - MERGE into target by key columns
    - REMOVE staged file
    """
    if not os.path.exists(file_path):
        print(f"[WARN] File not found: {file_path}. Skipping...")
        return

    file_name = os.path.basename(file_path)
    print(f"--- Processing {file_name} -> {table} ---")
    
    # Check if target table exists
    try:
        cs.execute(f"DESC TABLE {table};")
    except Exception as e:
        print(f"[ERROR] Target table {table} does not exist: {e}")
        return

    # Upload to user stage
    cs.execute(f"PUT file://{file_path} @~ auto_compress=false;")
    _ = cs.fetchall()

    # Get full column list
    cs.execute(f"DESC TABLE {table};")
    columns_info = cs.fetchall()
    column_names = [row[0] for row in columns_info]

    non_key_columns = [c for c in column_names if c not in key_columns]
    staging_table = f"{table}_STAGING"

    cs.execute(f"CREATE OR REPLACE TEMP TABLE {staging_table} AS SELECT * FROM {table} WHERE 1=0;")

    copy_sql = f"""
    COPY INTO {staging_table}
    FROM @~/{file_name}
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    FILE_FORMAT=(TYPE='JSON')
    ON_ERROR='CONTINUE';
    """
    cs.execute(copy_sql)
    copy_result = cs.fetchall()
    
    # Check how many rows were loaded into staging
    cs.execute(f"SELECT COUNT(*) FROM {staging_table};")
    staging_count = cs.fetchone()[0]
    
    if staging_count == 0:
        print(f"[WARN] No rows loaded into staging table. COPY result: {copy_result}")

    join_on = " AND ".join([f"t.{c}=s.{c}" for c in key_columns])
    set_clause = ", ".join([f"t.{c}=s.{c}" for c in non_key_columns])
    insert_cols = ", ".join(column_names)
    insert_vals = ", ".join([f"s.{c}" for c in column_names])

    merge_sql = f"""
    MERGE INTO {table} t
    USING {staging_table} s
      ON {join_on}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """
    cs.execute(merge_sql)
    merge_result = cs.fetchall()

    cs.execute(f"REMOVE @~/{file_name};")
    _ = cs.fetchall()

    print(f"[OK] Finished {file_name}.")

# =========================
# Main
# =========================
if __name__ == "__main__":

    
    if not META_ACCESS_TOKEN:
        print("[ERROR] META_ACCESS_TOKEN is not set!")
        sys.exit(1)
    
    if not AD_ACCOUNTS:
        print("[ERROR] No ad accounts configured in META_AD_ACCOUNTS!")
        sys.exit(1)

    # 1) Extract (configurable date range)
    try:
        # Default: rolling 7 days, but you can adjust these
        days_back = int(os.getenv("META_DAYS_BACK", "7"))
        until = date.today()  # Include today's data
        since = until - timedelta(days=days_back-1)
        


        block_campaign_by_day(since, until)
        block_campaign_by_hour(since, until)
        block_adset_platform_by_day(since, until)
        block_adset_demographics_by_day(since, until)
        block_campaign_metadata()

    except Exception as e:
        print(f"[ERROR] Extract failed: {e}")
        import traceback
        traceback.print_exc()
        send_email("URGENT! service_account SSH facebook/fb_to_snowflake.py extract failed",
                   f"Check logs. Error: {e}")
        sys.exit(1)


    print("Loading to Snowflake")
    # 2) Load
    # Map output files to target tables.
    # Example SANDBOX targets:
    file_to_table = {
        "campaign_by_day.json":  {
            "table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_CAMPAIGN_PERFORMANCE_BY_DAY",
            "keys":  ["CAMPAIGN_ID", "SEGMENTS_DATE"]  # add device/network if you include those breakdowns
        },
        "campaign_by_hour.json": {
            "table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_CAMPAIGN_PERFORMANCE_BY_HOUR",
            "keys":  ["CAMPAIGN_ID", "SEGMENTS_DATE", "SEGMENTS_HOUR"]
        },
        "adset_platform_by_day.json":     {
            "table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_ADSET_PLATFORM_BY_DAY",
            "keys":  ["ADSET_ID", "SEGMENTS_DATE", "SEGMENTS_DEVICE_PLATFORM", "SEGMENTS_PUBLISHER_PLATFORM"]
        },
        "adset_demographics_by_day.json":     {
            "table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_ADSET_DEMOGRAPHICS_BY_DAY",
            "keys":  ["ADSET_ID", "SEGMENTS_DATE", "SEGMENTS_AGE", "SEGMENTS_GENDER"]
        },
        "campaign_metadata.json": {
            "table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_CAMPAIGN_METADATA",
            "keys": ["CAMPAIGN_ID"]
        }
    }

    # Example “prod-style” mapping (uncomment & adjust):
    # file_to_table = {
    #     "campaign_by_day.json":  {"table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_CAMPAIGN_PERFORMANCE_BY_DAY", "keys": ["CAMPAIGN_ID","SEGMENTS_DATESTART"]},
    #     "campaign_by_hour.json": {"table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_CAMPAIGN_PERFORMANCE_BY_HOUR","keys": ["CAMPAIGN_ID","SEGMENTS_DATESTART","SEGMENTS_HOUR"]},
    #     "adset_by_day.json":     {"table": "REPLICA_PROD_3RDPARTY.FACEBOOK_ADS.FB_ADSET_PERFORMANCE_BY_DAY",     "keys": ["ADSET_ID","SEGMENTS_DATESTART","SEGMENTS_DEVICE_PLATFORM","SEGMENTS_PUBLISHER_PLATFORM"]}
    # }

    try:
        conn = open_snowflake_connection()
        cs = conn.cursor()

        # ensure DB/Schema active - use production schema
        cs.execute(f"USE ROLE {SNOWFLAKE_ROLE}")
        cs.execute("USE DATABASE REPLICA_PROD_3RDPARTY")
        cs.execute("USE SCHEMA FACEBOOK_ADS")

        for fname, meta in file_to_table.items():
            path = os.path.join(OUT_DIR, fname)
            load_file_to_table(cs, path, meta["table"], meta["keys"])

        conn.commit()
        cs.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Load failed: {e}")
        send_email("URGENT! service_account SSH facebook/fb_to_snowflake.py load failed",
                   f"Check logs. Error: {e}")
        sys.exit(2)

    print("[DONE] Facebook extraction + Snowflake load complete.")
