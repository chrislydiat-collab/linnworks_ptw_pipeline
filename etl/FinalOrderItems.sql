-- Step 1: Clear existing data for the source
DELETE FROM [linnworks].[lw].[final_orderitems]
WHERE source = 'linnworks';

-- Step 2: Insert transformed data
INSERT INTO [linnworks].[lw].[final_orderitems] (
    Final_sku,
    final_quantity,
    final_price,
    final_cost,
    TotalFinalPrice,
    TotalFinalCost,
    order_date,
    final_date,
    kitsku,
    source,
    Title,
    OrderId,
    LocationId
)
SELECT DISTINCT
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
            THEN OI.SubItemSKU
        ELSE OI.ParentSKU
    END,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemQty <> 0
            THEN OI.SubItemQty
        ELSE OI.ParentQty
    END,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemSellPrice <> 0
            THEN OI.SubItemSellPrice
        ELSE OI.ParentSellPrice
    END,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemUnitCost <> 0
            THEN OI.SubItemUnitCost
        ELSE OI.ParentUnitCost
    END,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
             AND OI.SubItemSellPrice <> 0 AND OI.SubItemQty <> 0
            THEN (OI.SubItemSellPrice * OI.SubItemQty)
        ELSE (OI.ParentSellPrice * OI.ParentQty)
    END,
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
             AND OI.SubItemUnitCost <> 0 AND OI.SubItemQty <> 0
            THEN (OI.SubItemUnitCost * OI.SubItemQty)
        ELSE (OI.ParentUnitCost * OI.ParentQty)
    END,
    O.dReceivedDate,
    O.dProcessedOn, -- final_date
    CASE
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
            THEN OI.ParentSKU
        ELSE NULL
    END,
    'linnworks',
    OI.ParentTitle,
    OI.OrderId,
    OI.LocationId
FROM [linnworks].[staging].[processed_orders] O
INNER JOIN [linnworks].[lw].[OrderItem_full] OI
    ON O.pkOrderId = OI.OrderId;