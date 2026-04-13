import os
import requests
import pymssql
from datetime import datetime
import logging
from flask import Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logging.basicConfig(level=logging.INFO)

# ------------------------------
# CONFIG
# ------------------------------
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")

# ------------------------------
# Helpers
# ------------------------------
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

# ------------------------------
# Linnworks API
# ------------------------------
def get_linnworks_access_token():
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {
        "ApplicationId": LINNWORKS_APP_ID,
        "ApplicationSecret": LINNWORKS_APP_SECRET,
        "Token": LINNWORKS_TOKEN
    }
    try:
        r = requests.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        token = data.get("Token") or data.get("AccessToken")
        if not token:
            logging.error("No token returned: %s", data)
            return None
        return token
    except Exception as e:
        logging.error("Error getting access token: %s", e)
        return None

def get_purchase_orders_summary(token):
    all_data = []
    page_number = 1
    total_pages = 1

    headers = {
        "Authorization": token,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    while page_number <= total_pages:
        payload = {
            "searchParameters": {
                "DateFrom": "2020-01-01T00:00:00",
                "DateTo": datetime.utcnow().isoformat()
            },
            "entriesPerPage": 100,
            "pageNumber": page_number
        }

        r = requests.post(
            "https://eu-ext.linnworks.net/api/PurchaseOrder/Search_PurchaseOrders2",
            json=payload,
            headers=headers
        )
        r.raise_for_status()
        json_data = r.json()
        results = json_data.get("Result", [])
        all_data.extend(results)
        total_pages = json_data.get("TotalPages", 1)
        page_number += 1

    logging.info("Fetched %d purchase order summaries.", len(all_data))
    return all_data

def get_purchase_order_details(pkPurchaseID, token):
    url = "https://eu-ext.linnworks.net/api/PurchaseOrder/Get_PurchaseOrder"
    headers = {
        "Authorization": token,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {"pkPurchaseId": pkPurchaseID}

    r = requests.post(url, json=payload, headers=headers)
    if r.status_code != 200:
        logging.warning("Failed to fetch PO %s", pkPurchaseID)
        return None
    return r.json()

def flatten_purchase_order(po):
    header = po.get("PurchaseOrderHeader", {})
    items = po.get("PurchaseOrderItem", [])
    delivered = po.get("DeliveredRecords", [])
    rows = []

    for item in items:
        matched_deliveries = [d for d in delivered if d.get("fkPurchaseItemId") == item.get("pkPurchaseItemId")]
        if matched_deliveries:
            for dr in matched_deliveries:
                rows.append(build_row(item, header, dr))
        else:
            rows.append(build_row(item, header, {}))
    return rows

def build_row(item, header, delivery):
    return {
        "PurchaseOrderItem_pkPurchaseItemId": item.get("pkPurchaseItemId"),
        "PurchaseOrderItem_fkStockItemId": item.get("fkStockItemId"),
        "PurchaseOrderItem_StockItemIntId": item.get("StockItemIntId"),
        "PurchaseOrderItem_Quantity": item.get("Quantity"),
        "PurchaseOrderItem_Cost": item.get("Cost"),
        "PurchaseOrderItem_Delivered": item.get("Delivered"),
        "PurchaseOrderItem_TaxRate": item.get("TaxRate"),
        "PurchaseOrderItem_Tax": item.get("Tax"),
        "PurchaseOrderItem_PackQuantity": item.get("PackQuantity"),
        "PurchaseOrderItem_PackSize": item.get("PackSize"),
        "PurchaseOrderItem_SKU": item.get("SKU"),
        "PurchaseOrderItem_ItemTitle": item.get("ItemTitle"),
        "PurchaseOrderItem_InventoryTrackingType": item.get("InventoryTrackingType"),
        "PurchaseOrderItem_IsDeleted": item.get("IsDeleted"),
        "PurchaseOrderItem_SortOrder": item.get("SortOrder"),
        "PurchaseOrderItem_DimHeight": item.get("DimHeight"),
        "PurchaseOrderItem_DimWidth": item.get("DimWidth"),
        "PurchaseOrderItem_BarcodeNumber": item.get("BarcodeNumber"),
        "PurchaseOrderItem_DimDepth": item.get("DimDepth"),
        "PurchaseOrderItem_BoundToOpenOrdersItems": item.get("BoundToOpenOrdersItems"),
        "PurchaseOrderItem_QuantityBoundToOpenOrdersItems": item.get("QuantityBoundToOpenOrdersItems"),
        "PurchaseOrderItem_SupplierCode": item.get("SupplierCode"),
        "PurchaseOrderItem_SupplierBarcode": item.get("SupplierBarcode"),
        "PurchaseOrderItem_SkuGroupIds": str(item.get("SkuGroupIds") or []),
        "PurchaseOrderHeader_pkPurchaseID": header.get("pkPurchaseID"),
        "PurchaseOrderHeader_ExternalInvoiceNumber": header.get("ExternalInvoiceNumber"),
        "PurchaseOrderHeader_Status": header.get("Status"),
        "PurchaseOrderHeader_DateOfPurchase": header.get("DateOfPurchase"),
        "PurchaseOrderHeader_DateOfDelivery": header.get("DateOfDelivery"),
        "PurchaseOrderHeader_TotalCost": header.get("TotalCost"),
        "DeliveredRecords_pkDeliveryRecordId": delivery.get("pkDeliveryRecordId"),
        "DeliveredRecords_fkPurchaseItemId": delivery.get("fkPurchaseItemId"),
        "DeliveredRecords_fkStockLocationId": delivery.get("fkStockLocationId"),
        "DeliveredRecords_UnitCost": delivery.get("UnitCost"),
        "DeliveredRecords_DeliveredQuantity": delivery.get("DeliveredQuantity"),
        "DeliveredRecords_CreatedDateTime": delivery.get("CreatedDateTime"),
        "DeliveredRecords_fkBatchInventoryId": delivery.get("fkBatchInventoryId"),
        "DeliveredRecords_ModifiedDateTime": delivery.get("ModifiedDateTime")
    }

# ------------------------------
# Parallel fetch
# ------------------------------
def fetch_all_purchase_orders_parallel(batch_size=10, sleep_between_batches=1.5):
    token = get_linnworks_access_token()
    if not token:
        logging.error("Failed to get Linnworks token")
        return []

    summary = get_purchase_orders_summary(token)
    all_rows = []
    total = len(summary)
    logging.info("Total purchase orders to fetch: %d", total)

    for start in range(0, total, batch_size):
        batch = summary[start:start + batch_size]

        def fetch_and_flatten(order):
            details = get_purchase_order_details(order.get("pkPurchaseID"), token)
            if details:
                return flatten_purchase_order(details)
            return []

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(fetch_and_flatten, order) for order in batch]
            for future in as_completed(futures):
                all_rows.extend(future.result())

        logging.info("Processed %d / %d purchase orders...", min(start + batch_size, total), total)
        time.sleep(sleep_between_batches)

    logging.info("Finished fetching all purchase orders. Total flattened rows: %d", len(all_rows))
    return all_rows

# ------------------------------
# Database load
# ------------------------------
def load_to_db(all_rows):
    if not all_rows:
        logging.info("No data to insert into DB.")
        return

    conn = pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE
    )
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE [staging].[FullPurchaseOrders]")

        columns = list(all_rows[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        sql = f"INSERT INTO [staging].[FullPurchaseOrders] ({','.join(columns)}) VALUES ({placeholders})"

        for row in all_rows:
            cursor.execute(sql, [row[col] for col in columns])

        conn.commit()
        logging.info("Inserted %d rows into [staging].[FullPurchaseOrders]", len(all_rows))
    except Exception as e:
        logging.error("Error loading DB: %s", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ------------------------------
# Cloud Run Function Entrypoint
# ------------------------------
def main_fullPurchaseOrders(request: Request):
    try:
        all_rows = fetch_all_purchase_orders_parallel()
        load_to_db(all_rows)
        return f"Successfully loaded {len(all_rows)} rows.", 200
    except Exception as e:
        logging.error("Error in main_fullPurchaseOrders: %s", e)
        return f"Error: {e}", 500
