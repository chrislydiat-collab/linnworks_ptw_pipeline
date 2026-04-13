import os
import requests
import pymssql
import pandas as pd
import json
import time
import logging
import random
import io

from datetime import datetime
from flask import Request
from contextlib import contextmanager
from functools import wraps
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

# =========================
# Configuration
# =========================

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")

GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_OBJECT_PATH = os.getenv("GCS_OBJECT_PATH")

# =========================
# Helpers (CRITICAL)
# =========================

def normalize_to_string(value):
    """
    Force ALL values to a stable string type.
    This prevents Airbyte schema inference failures.
    """
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)

# =========================
# Retry Decorator
# =========================

def retry_with_backoff(retries=3, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(
                        "Attempt %d failed: %s. Retrying in %.2fs",
                        attempt + 1, str(e)[:200], delay
                    )
                    time.sleep(delay)
        return wrapper
    return decorator

# =========================
# SQL Connection (Read-only)
# =========================

@contextmanager
def get_db_connection():
    conn = pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE
    )
    try:
        yield conn
    finally:
        conn.close()

def fetch_locations_from_sql():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT pkStockLocationId
            FROM [linnworks].[lw].[StockLocation]
        """)
        locations = [row[0] for row in cursor.fetchall()]
        cursor.close()

    logging.info("Fetched %d locations", len(locations))
    return locations

# =========================
# Linnworks API
# =========================

def get_linnworks_access_token():
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {
        "ApplicationId": LINNWORKS_APP_ID,
        "ApplicationSecret": LINNWORKS_APP_SECRET,
        "Token": LINNWORKS_TOKEN
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    token = response.json().get("Token")
    if not token:
        raise RuntimeError("Missing Linnworks access token")

    return token

@retry_with_backoff(retries=3, base_delay=2)
def fetch_orders_for_location(location_id, headers):
    url = "https://eu-ext.linnworks.net/api/OpenOrders/GetOpenOrderIds"
    payload = {
        "LocationId": str(location_id),
        "ViewId": 1,
        "EntriesPerPage": 50000
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    time.sleep(1)
    return response.json().get("Data", [])

@retry_with_backoff(retries=2, base_delay=1)
def fetch_order_details_batch(order_ids, headers):
    url = "https://eu-ext.linnworks.net/api/Orders/GetOrdersById"
    response = requests.post(
        url,
        json={"pkOrderIds": order_ids},
        headers=headers
    )
    response.raise_for_status()
    time.sleep(1.5)
    return response.json()

# =========================
# Core Fetch Logic
# =========================

def fetch_open_orders(access_token):
    location_ids = fetch_locations_from_sql()

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": access_token
    }

    open_order_ids = []
    for location_id in location_ids:
        try:
            open_order_ids.extend(
                fetch_orders_for_location(location_id, headers)
            )
        except Exception as e:
            logging.error("Location %s failed: %s", location_id, str(e))

    if not open_order_ids:
        return pd.DataFrame()

    rows = []
    batch_size = 100

    for i in range(0, len(open_order_ids), batch_size):
        orders = fetch_order_details_batch(
            open_order_ids[i:i + batch_size],
            headers
        )

        for order in orders:
            rows.append({
                "_airbyte_raw_id": "",
                "_airbyte_extracted_at": int(datetime.utcnow().timestamp() * 1000),
                "_airbyte_meta": "",
                "_airbyte_generation_id": "",

                "OrderId": normalize_to_string(order.get("OrderId")),
                "NumOrderId": normalize_to_string(order.get("NumOrderId")),
                "FolderName": normalize_to_string(order.get("FolderName")),
                "TaxId": normalize_to_string(order.get("TaxId")),
                "Processed": 0,
                "FulfilmentLocationId": normalize_to_string(order.get("FulfilmentLocationId")),
                "PaidDateTime": normalize_to_string(order.get("PaidDateTime")),
                "ProcessedDateTime": normalize_to_string(order.get("ProcessedDateTime")),

                "Items": normalize_to_string(order.get("Items")),
                "Notes": normalize_to_string(order.get("Notes")),
                "TotalsInfo": normalize_to_string(order.get("TotalsInfo")),
                "GeneralInfo": normalize_to_string(order.get("GeneralInfo")),
                "CustomerInfo": normalize_to_string(order.get("CustomerInfo")),
                "ShippingInfo": normalize_to_string(order.get("ShippingInfo")),
                "ExtendedProperties": normalize_to_string(order.get("ExtendedProperties")),
            })

    logging.info("Fetched %d orders", len(rows))
    return pd.DataFrame(rows)

# =========================
# GCS Upload (Overwrite)
# =========================

def upload_orders_to_gcs(df: pd.DataFrame):
    if df.empty:
        logging.info("No data to upload")
        return

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    buffer = io.StringIO()
    df.to_json(buffer, orient="records", lines=True)
    buffer.seek(0)

    blob = bucket.blob(GCS_OBJECT_PATH)
    blob.upload_from_string(
        buffer.getvalue(),
        content_type="application/json"
    )

    logging.info(
        "Uploaded %d records to gs://%s/%s",
        len(df), GCS_BUCKET, GCS_OBJECT_PATH
    )

# =========================
# Cloud Run Entry Point
# =========================

def main_openOrders(request: Request):
    start = time.time()
    try:
        logging.info("Starting Linnworks OpenOrders sync")
        token = get_linnworks_access_token()
        df = fetch_open_orders(token)
        upload_orders_to_gcs(df)
        return (f"Uploaded {len(df)} orders in {time.time() - start:.2f}s", 200)
    except Exception as e:
        logging.exception("Execution failed")
        return (str(e), 500)
