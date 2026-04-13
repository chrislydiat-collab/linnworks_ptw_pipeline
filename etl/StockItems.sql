INSERT INTO lw.StockItems (
    pkStockID,
    cItemNumber,
    cItemName,
    cDescription,
    fPricePerItem,
    fkStockControlStockItemId
)
SELECT 
    TRY_CAST(epDetails.pkRowId AS uniqueidentifier) AS pkStockID,
    s.ItemNumber AS cItemNumber,
    s.ItemTitle AS cItemName,
    s.ItemChannelDescriptions AS cDescription,
    TRY_CAST(s.RetailPrice AS DECIMAL(18,2)) AS fPricePerItem,
    epDetails.fkStockItemId AS fkStockControlStockItemId
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items] s
OUTER APPLY (
    SELECT 
        epValues.pkRowId,
        epValues.fkStockItemId
    FROM 
        OPENJSON(s.ItemExtendedProperties) AS ep
    CROSS APPLY 
        OPENJSON(ep.value) WITH (
            pkRowId uniqueidentifier '$.pkRowId',
            fkStockItemId uniqueidentifier '$.fkStockItemId'
        ) AS epValues
    WHERE ep.[key] = '0' OR ep.[key] IS NULL
) AS epDetails
WHERE 
    NOT EXISTS (
        SELECT 1 
        FROM lw.StockItems si
        WHERE si.pkStockID = TRY_CAST(epDetails.pkRowId AS uniqueidentifier)
    );