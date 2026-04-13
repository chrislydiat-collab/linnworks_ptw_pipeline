import os
import requests
import pymssql
from datetime import datetime
import logging
from flask import jsonify

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
# Helper Functions
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
    return str(value).strip().lower()

# ------------------------------
# Linnworks API Functions
# ------------------------------
def get_linnworks_access_token():
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {
        "ApplicationId": LINNWORKS_APP_ID,
        "ApplicationSecret": LINNWORKS_APP_SECRET,
        "Token": LINNWORKS_TOKEN
    }
    try:
        response = requests.post(url, json=payload)
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

def get_linnworks_purchase_orders():
    token = get_linnworks_access_token()
    if not token:
        return []

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

        response = requests.post(
            "https://eu-ext.linnworks.net/api/PurchaseOrder/Search_PurchaseOrders2",
            json=payload,
            headers=headers
        )
        response.raise_for_status()
        json_data = response.json()

        results = json_data.get("Result", [])
        all_data.extend(results)

        total_pages = json_data.get("TotalPages", 1)
        page_number += 1

    logging.info("Fetched %d purchase orders from Linnworks.", len(all_data))
    return all_data

# ------------------------------
# Database Function
# ------------------------------
def push_linnworks_data_to_mssql(all_data):
    if not all_data:
        logging.info("No data to insert.")
        return

    conn = pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE
    )
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT pkPurchaseID FROM lw.PurchaseOrders")
        existing_ids = {safe_str(row[0]) for row in cursor.fetchall() if row[0]}

        new_records = [
            r for r in all_data
            if r.get("pkPurchaseID") and safe_str(r["pkPurchaseID"]) not in existing_ids
        ]
        logging.info("New records to insert: %d", len(new_records))

        if not new_records:
            return

        sql = """
        INSERT INTO lw.PurchaseOrders (
            pkPurchaseID, fkSupplierId, fkLocationId, ExternalInvoiceNumber, Status, Currency, SupplierReferenceNumber,
            Locked, LineCount, DeliveredLinesCount, UnitAmountTaxIncludedType, DateOfPurchase, DateOfDelivery, QuotedDeliveryDate,
            PostagePaid, TotalCost, taxPaid, ShippingTaxRate, ConversionRate, ConvertedShippingCost,
            ConvertedShippingTax, ConvertedOtherCost, ConvertedOtherTax, ConvertedGrandTotal
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        for row in new_records:
            cursor.execute(sql, (
                row.get("pkPurchaseID"),
                row.get("fkSupplierId"),
                row.get("fkLocationId"),
                row.get("ExternalInvoiceNumber"),
                row.get("Status"),
                row.get("Currency"),
                row.get("SupplierReferenceNumber"),
                1 if row.get("Locked") else 0,
                row.get("LineCount") or 0,
                row.get("DeliveredLinesCount") or 0,
                str(row.get("UnitAmountTaxIncludedType") or ""),
                safe_date(row.get("DateOfPurchase")),
                safe_date(row.get("DateOfDelivery")),
                safe_date(row.get("QuotedDeliveryDate")),
                row.get("PostagePaid") or 0,
                row.get("TotalCost") or 0,
                row.get("taxPaid") or 0,
                row.get("ShippingTaxRate") or 0,
                row.get("ConversionRate") or 0,
                row.get("ConvertedShippingCost") or 0,
                row.get("ConvertedShippingTax") or 0,
                row.get("ConvertedOtherCost") or 0,
                row.get("ConvertedOtherTax") or 0,
                row.get("ConvertedGrandTotal") or 0
            ))

        conn.commit()
        logging.info("Inserted %d new purchase orders.", len(new_records))

    except Exception as e:
        logging.error("Error during MSSQL insert: %s", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ------------------------------
# Cloud Run Function Entrypoint
# ------------------------------
def main_stocklevel(request):
    """
    HTTP-triggered Cloud Run Function.
    """
    try:
        all_data = get_linnworks_purchase_orders()
        push_linnworks_data_to_mssql(all_data)
        return "Purchase orders processed successfully.", 200
    except Exception as e:
        logging.error("Error in main_stocklevel: %s", e)
        return f"Error: {e}", 500
