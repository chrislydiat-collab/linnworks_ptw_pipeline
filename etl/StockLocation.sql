INSERT INTO lw.stocklocation (
    pkStockLocationId,
    Location,
    Address1,
    Address2,
    City,
    County,
    Country,
    ZipCode,
    bLogicalDelete,
    IsNotTrackable,
    LocationTag,
    IsFulfillmentCenter,
    CountInOrderUntilAcknowledgement,
    FulfilmentCenterDeductStockWhenProcessed,
    IsWarehouseManaged
)
SELECT
    TRY_CAST(StockLocationId AS UNIQUEIDENTIFIER) AS pkStockLocationId,
    LocationName AS Location,
    Address1,
    Address2,
    City,
    County,
    Country,
    ZipCode,
    NULL AS bLogicalDelete,
    TRY_CAST(IsNotTrackable AS BIT) AS IsNotTrackable,
    LocationTag,
    TRY_CAST(IsFulfillmentCenter AS BIT) AS IsFulfillmentCenter,
    TRY_CAST(NULLIF(LTRIM(RTRIM(CountInOrderUntilAcknowledgement)), '') AS INT) AS CountInOrderUntilAcknowledgement,
    TRY_CAST(FulfilmentCenterDeductStockWhenProcessed AS BIT) AS FulfilmentCenterDeductStockWhenProcessed,
    TRY_CAST(IsWarehouseManaged AS BIT) AS IsWarehouseManaged
FROM 
    [linnworks].[staging]._airbyte_raw_stock_locations AS data
WHERE 
    StockLocationId IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM lw.stocklocation s
        WHERE s.pkStockLocationId = TRY_CAST(data.StockLocationId AS UNIQUEIDENTIFIER)
    );
