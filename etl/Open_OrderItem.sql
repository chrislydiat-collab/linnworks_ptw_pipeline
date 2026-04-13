INSERT INTO lw.Open_OrderItem (
    Rowid,
    fkOrderId,
    fPricePerUnit,
    nQty,
    ItemNumber,
    fkLocationId,
    fkCompositeParentRowId,
    ChannelSKU,
    fkStockItemId,
    SalesTax,
    TaxRate,
    TaxCostInclusive,
    Cost,
    CostIncTax,
    PartShipped,
    LineDiscount,
    IsService,
    ItemSource,
    PartShippedQty,
    OriginalTitle,
    AffectingStockLevel,
    AvailableStock
)
SELECT
    item.RowId AS Rowid,
    TRY_CAST(data.OrderId AS UNIQUEIDENTIFIER) AS fkOrderId,
    item.PricePerUnit AS fPricePerUnit,
    SUM(BinRackData.Quantity) AS nQty,
    item.ItemNumber AS ItemNumber,
    TRY_CAST(data.FulfilmentLocationId AS UNIQUEIDENTIFIER) AS fkLocationId,
    NULL AS fkCompositeParentRowId,
    item.ChannelSKU AS ChannelSKU,
    item.FkStockItemId AS fkStockItemId,
    item.SalesTax AS SalesTax,
    item.TaxRate AS TaxRate,
    item.TaxCostInclusive AS TaxCostInclusive,
    item.Cost AS Cost,
    item.CostIncTax AS CostIncTax,
    item.PartShipped AS PartShipped,
    item.Discount AS LineDiscount,
    item.IsService AS IsService,
    item.ItemSource AS ItemSource,
    item.PartShippedQty AS PartShippedQty,
    item.Title AS OriginalTitle,
    item.StockLevelIndicator AS AffectingStockLevel,
    item.AvailableStock AS AvailableStock
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details AS data
CROSS APPLY OPENJSON(CAST(data.Items AS nvarchar(max)))
WITH (
    RowId UNIQUEIDENTIFIER,
    ItemNumber NVARCHAR(50),
    ChannelSKU NVARCHAR(50),
    FkStockItemId UNIQUEIDENTIFIER '$.StockItemId',
    PricePerUnit FLOAT,
    SalesTax FLOAT,
    TaxRate FLOAT,
    TaxCostInclusive BIT,
    Cost FLOAT,
    CostIncTax FLOAT,
    PartShipped BIT,
    Discount FLOAT,
    IsService BIT,
    ItemSource NVARCHAR(50),
    PartShippedQty INT,
    Title NVARCHAR(255),
    StockLevelIndicator INT,
    AvailableStock INT,
    BinRacks nvarchar(max) AS JSON
) AS item
OUTER APPLY OPENJSON(item.BinRacks)
WITH (
    Quantity INT '$.Quantity'
) AS BinRackData
WHERE NOT EXISTS (
    SELECT 1 
    FROM lw.Open_OrderItem oi
    WHERE oi.Rowid = item.RowId
)
GROUP BY
    item.RowId,
    data.OrderId,
    item.PricePerUnit,
    item.ItemNumber,
    data.FulfilmentLocationId,
    item.ChannelSKU,
    item.FkStockItemId,
    item.SalesTax,
    item.TaxRate,
    item.TaxCostInclusive,
    item.Cost,
    item.CostIncTax,
    item.PartShipped,
    item.Discount,
    item.IsService,
    item.ItemSource,
    item.PartShippedQty,
    item.Title,
    item.StockLevelIndicator,
    item.AvailableStock;
