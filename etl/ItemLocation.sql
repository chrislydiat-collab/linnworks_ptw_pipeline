INSERT INTO lw.ItemLocation (
    fkStockItemId, 
    fkLocationId, 
    BinRackNumber, 
    rowid
)
SELECT
    JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), '$[0].StockItemId') AS fkStockItemId,
    d.[FulfilmentLocationId] AS fkLocationId,
    COALESCE(
        NULLIF(JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), '$[0].BinRack'), ''),
        JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), '$[0].BinRacks[1].BinRack')
    ) AS BinRackNumber,
    JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), '$[0].RowId') AS rowid
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] d;
