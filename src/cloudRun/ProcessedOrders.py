import os
import json
import io
import pandas as pd
import pymssql
import logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def process_csv_gcs(request):
    logging.info("Cloud Function triggered.")

    client = storage.Client()
    bucket_name = "linnworks-processed-orders"
    folder_prefix = "processed-orders/processed_orders/"
    bucket = client.bucket(bucket_name)

    try:
        # List all blobs and get the latest
        blobs = list(client.list_blobs(bucket, prefix=folder_prefix))
        if not blobs:
            logging.warning("No CSV files found in the folder.")
            return "No CSV files found in the folder."

        latest_blob = max(blobs, key=lambda b: b.updated)
        logging.info(f"Processing latest CSV file: {latest_blob.name}")
        data = latest_blob.download_as_bytes()

        # Read CSV
        df = pd.read_csv(io.BytesIO(data), sep=',', quotechar='"')
        df.columns = df.columns.str.strip()

        # Parse _airbyte_data JSON
        if '_airbyte_data' not in df.columns:
            logging.error("_airbyte_data column not found in CSV.")
            return "_airbyte_data column not found in CSV."

        df_json = df['_airbyte_data'].apply(json.loads).apply(pd.Series)
        df_combined = pd.concat([df, df_json], axis=1)

        # SQL credentials
        SQL_SERVER = os.getenv("SQL_SERVER")
        SQL_USER = os.getenv("SQL_USER")
        SQL_PASSWORD = os.getenv("SQL_PASSWORD")
        SQL_DATABASE = os.getenv("SQL_DATABASE")

        logging.info(f"Connecting to SQL Server: {SQL_SERVER}")
        conn = pymssql.connect(
            server=SQL_SERVER,
            user=SQL_USER,
            password=SQL_PASSWORD,
            database=SQL_DATABASE
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT pkOrderID 
            FROM [linnworks].[staging].[processed_orders]
            WHERE status = 'processed'
        """)
        existing_ids = set(row[0] for row in cursor.fetchall())
        logging.info(f"Fetched {len(existing_ids)} existing processed pkOrderIDs from SQL Server.")

        # Filter only new rows
        df_new = df_combined[~df_combined['pkOrderID'].isin(existing_ids)]

        if df_new.empty:
            logging.info("No new rows to insert.")
            cursor.close()
            conn.close()
            return "No new rows to insert."

        # Prepare for bulk insert 
        insert_sql = """
        INSERT INTO [linnworks].[staging].[processed_orders]
        (pkOrderID, dProcessedOn, dReceivedDate, status)
        VALUES (%s, %s, %s, %s)
        """

        data_to_insert = [
            (row['pkOrderID'], row['dProcessedOn'], row['dReceivedDate'], 'processed')
            for _, row in df_new.iterrows()
        ]

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Inserted {len(df_new)} new rows into SQL Server successfully.")
        return f"Inserted {len(df_new)} new rows into SQL Server successfully."

    except Exception as e:
        logging.exception("An error occurred during CSV processing.")
        return f"An error occurred: {str(e)}"
