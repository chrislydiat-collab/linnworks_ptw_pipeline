import os
import requests
import pymssql
import pandas as pd
import json
import math
import time
import logging
import random
from datetime import datetime, timezone
from flask import Request
from contextlib import contextmanager
from functools import wraps

logging.basicConfig(level=logging.INFO)

# Config 
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")

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
                    # Adding jittering here to prevent thundering herd
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"Attempt {attempt + 1} failed: {str(e)[:100]}... Retrying in {delay:.2f}s")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# Database connection manager
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pymssql.connect(
            server=SQL_SERVER, 
            user=SQL_USER, 
            password=SQL_PASSWORD, 
            database=SQL_DATABASE,
            timeout=60,
            login_timeout=30
        )
        yield conn
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@retry_with_backoff(retries=3, base_delay=2)
def fetch_orders_for_location(location_id, headers):
    url_open_orders = "https://eu-ext.linnworks.net/api/OpenOrders/GetOpenOrderIds"
    payload = {
        "LocationId": str(location_id),
        "ViewId": 1,
        "EntriesPerPage": 50000 
    }
    
    response = requests.post(url_open_orders, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    
    # Check for rate limit in response
    if response.status_code == 429:
        raise Exception("Rate limit exceeded")
    
    data = response.json().get("Data", [])
    
    # Rate limiting - increased delay
    time.sleep(1.0)  # 1 second between location requests
    return data

@retry_with_backoff(retries=2, base_delay=1)
def fetch_order_details_batch(batch_ids, headers):
    url_order_details = "https://eu-ext.linnworks.net/api/Orders/GetOrdersById"
    payload = {"pkOrderIds": batch_ids}
    
    response = requests.post(url_order_details, json=payload, headers=headers, timeout=45)
    response.raise_for_status()
    
    if response.status_code == 429:
        raise Exception("Rate limit exceeded")
    
    orders = response.json()
    
    # Rate limiting between batches,  somrthing to watch how it progresses
    time.sleep(1.5)  
    return orders

def fetch_open_orders(cursor, access_token):
    cursor.execute("SELECT DISTINCT pkStockLocationId FROM [linnworks].[lw].[StockLocation]")
    location_ids = [row[0] for row in cursor.fetchall()]
    logging.info("Found %d locations", len(location_ids))

    headers = {
        "accept": "application/json",
        "content-type": "application/json", 
        "Authorization": access_token
    }

    open_order_ids = []
    failed_locations = []

    # Process locations with error handling
    for i, location_id in enumerate(location_ids):
        try:
            data = fetch_orders_for_location(location_id, headers)
            open_order_ids.extend(data)
            logging.info("Fetched %d orders for location %s (%d/%d)", 
                       len(data), location_id, i+1, len(location_ids))
        except Exception as e:
            logging.error("Failed to fetch orders for location %s: %s", location_id, str(e)[:100])
            failed_locations.append(str(location_id))
            # Continue processing other locations
            continue

    if failed_locations:
        logging.warning("Failed to fetch from %d locations: %s", 
                      len(failed_locations), failed_locations[:3])  

    if not open_order_ids:
        logging.info("No order IDs fetched from any location")
        return pd.DataFrame()

    
    batch_size = 100  
    total_batches = math.ceil(len(open_order_ids) / batch_size)
    orders_data = []
    failed_batches = 0

    logging.info("Fetching details for %d orders in %d batches", len(open_order_ids), total_batches)

    for i in range(total_batches):
        batch_ids = open_order_ids[i * batch_size:(i + 1) * batch_size]
        
        try:
            orders = fetch_order_details_batch(batch_ids, headers)
            
            for order in orders:
                orders_data.append({
                    "_airbyte_raw_id": None,
                    "_airbyte_extracted_at": int(datetime.now().timestamp() * 1000),  
                    "_airbyte_meta": None,
                    "_airbyte_generation_id": None,
                    "Items": json.dumps(order.get("Items")),
                    "Notes": json.dumps(order.get("Notes")),
                    "TaxId": order.get("TaxId") or '',
                    "OrderId": order.get("OrderId") or '',
                    "Processed": 0,
                    "FolderName": order.get("FolderName") or '',
                    "NumOrderId": order.get("NumOrderId"),
                    "TotalsInfo": json.dumps(order.get("TotalsInfo")),
                    "GeneralInfo": json.dumps(order.get("GeneralInfo")),
                    "CustomerInfo": json.dumps(order.get("CustomerInfo")),
                    "PaidDateTime": order.get("PaidDateTime"),
                    "ShippingInfo": json.dumps(order.get("ShippingInfo")),
                    "ProcessedDateTime": order.get("ProcessedDateTime"),
                    "ExtendedProperties": json.dumps(order.get("ExtendedProperties")),
                    "FulfilmentLocationId": order.get("FulfilmentLocationId") or ''
                })
                
            logging.info("Processed batch %d/%d (%d orders)", i + 1, total_batches, len(orders))
            
        except Exception as e:
            logging.error("Error fetching batch %d: %s", i + 1, str(e)[:100])
            failed_batches += 1
            # Continue with next batch instead of failing completely
            continue

    df_orders = pd.DataFrame(orders_data)
    logging.info("Total orders fetched: %d (failed batches: %d, failed locations: %d)", 
                len(df_orders), failed_batches, len(failed_locations))
    return df_orders

def insert_orders_to_sql(df, conn):
    if df.empty:
        logging.info("No orders to insert.")
        return

    cursor = conn.cursor()
    sql = """
    INSERT INTO [linnworks].[staging].[_airbyte_raw_processed_order_details] (
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, _airbyte_generation_id,
        Items, Notes, TaxId, OrderId, Processed, FolderName, NumOrderId,
        TotalsInfo, GeneralInfo, CustomerInfo, PaidDateTime, ShippingInfo,
        ProcessedDateTime, ExtendedProperties, FulfilmentLocationId
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row["_airbyte_raw_id"],
                row["_airbyte_extracted_at"], 
                row["_airbyte_meta"],
                row["_airbyte_generation_id"],
                row["Items"],
                row["Notes"],
                row["TaxId"],
                row["OrderId"],
                row["Processed"],
                row["FolderName"],
                row["NumOrderId"],
                row["TotalsInfo"],
                row["GeneralInfo"],
                row["CustomerInfo"],
                row["PaidDateTime"],
                row["ShippingInfo"],
                row["ProcessedDateTime"],
                row["ExtendedProperties"],
                row["FulfilmentLocationId"]
            ))
        
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        logging.info("Inserted %d orders into SQL.", len(df))
        
    except Exception as e:
        logging.error("Error inserting orders into SQL: %s", e)
        conn.rollback()
        raise
    finally:
        cursor.close()

def main_openOrders(request: Request):
    start_time = datetime.now()
    
    try:
        logging.info("Starting OpenOrders processing...")

        access_token = get_linnworks_access_token()
        if not access_token:
            return ("Failed to get Linnworks access token.", 500)

        with get_db_connection() as conn:
            cursor = conn.cursor()
            completed, last_run_dt = airbyte_completed_today(cursor)

            if not completed:
                msg = f"Airbyte last run was {last_run_dt}, not today." if last_run_dt else "No Airbyte runs detected yet."
                logging.info(msg)
                return (msg, 200)

            clear_unprocessed_orders(conn)
            df_orders = fetch_open_orders(cursor, access_token)
            
            if not df_orders.empty:
                insert_orders_to_sql(df_orders, conn)

        elapsed = (datetime.now() - start_time).total_seconds()
        logging.info("Processing completed in %.2f seconds", elapsed)
        
        return (f"OpenOrders refreshed successfully. Inserted {len(df_orders)} rows.", 200)

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logging.error("Error in main_openOrders after %.2f seconds: %s", elapsed, e)
        return (f"Error: {e}", 500)

def safe_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_str(value):
    if value is None:
        return None
    return str(value).strip()

def get_linnworks_access_token():
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {
        "ApplicationId": LINNWORKS_APP_ID,
        "ApplicationSecret": LINNWORKS_APP_SECRET,
        "Token": LINNWORKS_TOKEN
    }
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        token = data.get("Token") or data.get("AccessToken")
        if not token:
            logging.error("Access token not found in response: %s", data)
            return None
        logging.info("Fetched Linnworks access token successfully.")
        return token
    except Exception as e:
        logging.error("Error fetching Linnworks access token: %s", e)
        return None

def airbyte_completed_today(cursor):
    cursor.execute("""
        SELECT MAX(_airbyte_extracted_at)
        FROM [linnworks].[staging].[_airbyte_raw_processed_order_details]
    """)
    last_run_ms = cursor.fetchone()[0]
    if not last_run_ms:
        return False, None
    last_run_dt = datetime.fromtimestamp(last_run_ms / 1000, tz=timezone.utc)
    today = datetime.now(timezone.utc).date()
    return last_run_dt.date() == today, last_run_dt

def clear_unprocessed_orders(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM [linnworks].[staging].[_airbyte_raw_processed_order_details]
            WHERE Processed = 0
        """)
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        logging.info("Cleared %d old unprocessed records before inserting new ones.", affected)
    except Exception as e:
        logging.error("Error clearing old unprocessed records: %s", e)
        conn.rollback()