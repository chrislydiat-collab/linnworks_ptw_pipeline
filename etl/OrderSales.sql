INSERT INTO [linnworks].[lw].[Order_sales] (
    OrderDate,
    DispatchDate,
    itemID,
    UnitCost,
    TotalCost,
    UnitPrice,
    TotalPrice,
    UnitTax,
    TotalTax,
    TotalIncTax,
    Quantity,
    SubItemSKU,
    SubItemUnitCost,
    SubItemQty,
    fkOrderId,
    FkLocationId,
    ItemSource,
    fkStockItemId,
    source
)
SELECT DISTINCT
    TRY_CAST(O.dReceivedDate AS DATETIME) AS OrderDate,
    TRY_CAST(O.dProcessedOn AS DATETIME) AS DispatchDate,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
            THEN OI.SubItemSKU
        ELSE OI.ParentSKU
    END AS itemID,
    OI.ParentUnitCost AS UnitCost,
    OI.ParentUnitCost * OI.ParentQty AS TotalCost,
    OI.ParentSellPrice AS UnitPrice,
    OI.ParentSellPrice * OI.ParentQty AS TotalPrice,
    OI.ParentTax / NULLIF(OI.ParentQty, 0) AS UnitTax,
    OI.ParentTax AS TotalTax,
    OI.ParentTotalIncTax AS TotalIncTax,
    OI.ParentQty AS Quantity,
    OI.SubItemSKU AS SubItemSKU,
    OI.SubItemUnitCost AS SubItemUnitCost,
    OI.SubItemQty AS SubItemQty,
    CAST(OI.OrderId AS VARCHAR(36)) AS fkOrderId,
    OI.LocationId AS FkLocationId,
    OI.ParentItemSource AS ItemSource,
    CAST(OI.ParentStockItemId AS VARCHAR(36)) AS fkStockItemId,
    'linnworks' AS source
FROM [linnworks].[staging].[processed_orders] O
LEFT JOIN [linnworks].[lw].[OrderItem_full] OI
    ON O.pkOrderID = CAST(OI.OrderId AS VARCHAR(255));
