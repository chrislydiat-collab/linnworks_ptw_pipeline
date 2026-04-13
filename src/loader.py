import os
import io
import json
import time
import logging
import requests
import pandas as pd
import pymssql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from flask import Request

logging.basicConfig(level=logging.INFO)

# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

LINNWORKS_APP_ID = os.getenv("LINNWORKS_APP_ID")
LINNWORKS_APP_SECRET = os.getenv("LINNWORKS_APP_SECRET")
LINNWORKS_TOKEN = os.getenv("LINNWORKS_TOKEN")

BUCKET_NAME = os.getenv("BUCKET_NAME", "linnworks-processed-orders")
FOLDER_PREFIX = os.getenv("FOLDER_PREFIX", "processed-orders/processed_orders/")

# =============================================================================
# UTILITIES
# =============================================================================
def safe_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_str(value):
    return str(value).strip().lower() if value else None


# =============================================================================
# GCS → SQL Server (Processed Orders)
# =============================================================================
def process_csv_from_gcs():
    """Reads latest CSV from GCS, parses JSON column, and loads into SQL Server."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blobs = list(client.list_blobs(bucket, prefix=FOLDER_PREFIX))
        if not blobs:
            logging.warning("No CSV files found in folder.")
            return "No CSV files found."

        latest_blob = max(blobs, key=lambda b: b.updated)
        data = latest_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(data), sep=',', quotechar='"')
        df.columns = df.columns.str.strip()

        if '_airbyte_data' not in df.columns:
            logging.warning("_airbyte_data column missing.")
            return "_airbyte_data column not found."

        df_json = df['_airbyte_data'].apply(json.loads).apply(pd.Series)
        df_combined = pd.concat([df, df_json], axis=1)

        conn = pymssql.connect(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM [linnworks].[staging].[processed_orders]")
        conn.commit()

        insert_sql = """
        INSERT INTO [linnworks].[staging].[processed_orders]
        (pkOrderID, dProcessedOn, dReceivedDate)
        VALUES (%s, %s, %s)
        """
        for row in df_combined.itertuples(index=False):
            cursor.execute(insert_sql, (row.pkOrderID, row.dProcessedOn, row.dReceivedDate))
        conn.commit()

        cursor.close()
        conn.close()

        msg = f"Inserted {len(df_combined)} rows from GCS into SQL Server."
        logging.info(msg)
        return msg

    except Exception as e:
        logging.error("Error processing GCS CSV: %s", e)
        return f"Error processing GCS CSV: {e}"


# =============================================================================
# LINNWORKS API
# =============================================================================
class LinnworksAPI:
    def __init__(self, app_id, app_secret, token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = token
        self.access_token = None

    def get_access_token(self):
        if self.access_token:
            return self.access_token
        url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
        payload = {"ApplicationId": self.app_id, "ApplicationSecret": self.app_secret, "Token": self.token}
        try:
            r = requests.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            self.access_token = data.get("Token") or data.get("AccessToken")
            return self.access_token
        except Exception as e:
            logging.error("Error getting access token: %s", e)
            return None

    def get_purchase_orders_summary(self):
        token = self.get_access_token()
        if not token:
            return []
        all_data, page_number, total_pages = [], 1, 1
        headers = {"Authorization": token, "Accept": "application/json", "Content-Type": "application/json"}
        while page_number <= total_pages:
            payload = {"searchParameters": {"DateFrom": "2020-01-01T00:00:00",
                                            "DateTo": datetime.utcnow().isoformat()},
                       "entriesPerPage": 100, "pageNumber": page_number}
            r = requests.post("https://eu-ext.linnworks.net/api/PurchaseOrder/Search_PurchaseOrders2",
                              json=payload, headers=headers)
            r.raise_for_status()
            json_data = r.json()
            all_data.extend(json_data.get("Result", []))
            total_pages = json_data.get("TotalPages", 1)
            page_number += 1
        logging.info("Fetched %d purchase order summaries.", len(all_data))
        return all_data

    def get_purchase_order_details(self, pkPurchaseID):
        token = self.get_access_token()
        if not token:
            return None
        url = "https://eu-ext.linnworks.net/api/PurchaseOrder/Get_PurchaseOrder"
        headers = {"Authorization": token, "Accept": "application/json", "Content-Type": "application/json"}
        payload = {"pkPurchaseId": pkPurchaseID}
        r = requests.post(url, json=payload, headers=headers)
        return r.json() if r.status_code == 200 else None

    def flatten_purchase_order(self, po):
        header = po.get("PurchaseOrderHeader", {})
        items = po.get("PurchaseOrderItem", [])
        delivered = po.get("DeliveredRecords", [])
        rows = []
        for item in items:
            matched = [d for d in delivered if d.get("fkPurchaseItemId") == item.get("pkPurchaseItemId")]
            if matched:
                for dr in matched:
                    rows.append(self.build_row(item, header, dr))
            else:
                rows.append(self.build_row(item, header, {}))
        return rows

    @staticmethod
    def build_row(item, header, delivery):
        """Full flattened row structure for DB insertion."""
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
            "DeliveredRecords_ModifiedDateTime": delivery.get("ModifiedDateTime"),
        }

    def fetch_all_purchase_orders_parallel(self, batch_size=10, sleep_between_batches=1.5):
        summary = self.get_purchase_orders_summary()
        all_rows = []
        total = len(summary)
        logging.info("Total POs to fetch: %d", total)
        for start in range(0, total, batch_size):
            batch = summary[start:start + batch_size]
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = [executor.submit(lambda order=o: self.flatten_purchase_order(
                    self.get_purchase_order_details(order.get("pkPurchaseID")))) for o in batch]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        all_rows.extend(result)
            time.sleep(sleep_between_batches)
        return all_rows


# =============================================================================
# DATABASE LOADER
# =============================================================================
class DatabaseLoader:
    def __init__(self):
        self.conn = pymssql.connect(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE)

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

    def push_stock_orders(self, records):
        if not records:
            logging.info("No stock orders to insert.")
            return
        cursor = self.conn.cursor()
        cursor.execute("SELECT pkPurchaseID FROM lw.PurchaseOrders")
        existing_ids = {safe_str(row[0]) for row in cursor.fetchall() if row[0]}
        new_records = [r for r in records if r.get("pkPurchaseID") and safe_str(r["pkPurchaseID"]) not in existing_ids]
        if not new_records:
            logging.info("No new stock orders.")
            return
        sql = """INSERT INTO lw.PurchaseOrders (pkPurchaseID, Status, DateOfPurchase)
                 VALUES (%s, %s, %s)"""
        for row in new_records:
            cursor.execute(sql, (row.get("pkPurchaseID"), row.get("Status"), safe_date(row.get("DateOfPurchase"))))
        self.conn.commit()
        logging.info("Inserted %d stock orders.", len(new_records))

    def load_full_purchase_orders(self, all_rows):
        if not all_rows:
            return
        cursor = self.conn.cursor()
        cursor.execute("TRUNCATE TABLE [staging].[FullPurchaseOrders]")
        columns = list(all_rows[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        sql = f"INSERT INTO [staging].[FullPurchaseOrders] ({','.join(columns)}) VALUES ({placeholders})"
        for row in all_rows:
            cursor.execute(sql, [row[col] for col in columns])
        self.conn.commit()
        logging.info("Inserted %d rows into [staging].[FullPurchaseOrders]", len(all_rows))
        cursor.close()


# =============================================================================
# PIPELINE RUNNER
# =============================================================================
def run_linnworks_pipeline(api: LinnworksAPI, db_loader: DatabaseLoader):
    stock_orders = api.get_purchase_orders_summary()
    db_loader.push_stock_orders(stock_orders)
    full_po_rows = api.fetch_all_purchase_orders_parallel()
    db_loader.load_full_purchase_orders(full_po_rows)
    return f"Linnworks data loaded: {len(full_po_rows)} full POs"


# =============================================================================
# MAIN CLOUD FUNCTION ENTRY POINT
# =============================================================================
def linnworks_full_loader(request: Request):
    """Cloud Function that loads both Linnworks data and GCS CSVs in parallel."""
    try:
        api = LinnworksAPI(LINNWORKS_APP_ID, LINNWORKS_APP_SECRET, LINNWORKS_TOKEN)
        db_loader = DatabaseLoader()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(process_csv_from_gcs): "GCS → SQL",
                executor.submit(run_linnworks_pipeline, api, db_loader): "Linnworks → SQL"
            }
            results = {}
            for future in as_completed(futures):
                task_name = futures[future]
                try:
                    results[task_name] = future.result()
                except Exception as e:
                    results[task_name] = f"Error: {e}"

        logging.info("Parallel execution results: %s", results)
        return results, 200
    except Exception as e:
        logging.error("Error in linnworks_full_loader: %s", e)
        return f"Error: {e}", 500
