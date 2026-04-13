INSERT INTO lw.StockItem_ExtendedProperties (
    pkRowId,
    fkStockItemId,
    PropertyName,
    PropertyValue,
    PropertyType
)
SELECT
    TRY_CAST(ep.pkRowId AS UNIQUEIDENTIFIER) AS pkRowId,
    TRY_CAST(d.StockItemId AS UNIQUEIDENTIFIER) AS fkStockItemId,
    ep.ProperyName AS PropertyName,  -- Alias the misspelled JSON key here
    ep.PropertyValue,
    ep.PropertyType
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items] d
CROSS APPLY 
    OPENJSON(d.ItemExtendedProperties) 
    WITH (
        pkRowId VARCHAR(255) '$.pkRowId',
        ProperyName VARCHAR(255) '$.ProperyName',  -- Original key from JSON
        PropertyValue VARCHAR(255) '$.PropertyValue',
        PropertyType VARCHAR(255) '$.PropertyType'
    ) ep
WHERE d.ItemExtendedProperties IS NOT NULL;
