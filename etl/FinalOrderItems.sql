DELETE FROM [linnworks].[lw].[final_orderitems]
WHERE source = 'linnworks';

INSERT INTO [linnworks].[lw].[final_orderitems] (
    Final_sku,
    final_quantity,
    final_price,
    final_cost,
    TotalFinalPrice,
    TotalFinalCost,
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
    END AS final_sku,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemQty <> 0
            THEN OI.SubItemQty
        ELSE OI.ParentQty
    END AS final_quantity,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemSellPrice <> 0
            THEN OI.SubItemSellPrice
        ELSE OI.ParentSellPrice
    END AS final_price,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' AND OI.SubItemUnitCost <> 0
            THEN OI.SubItemUnitCost
        ELSE OI.ParentUnitCost
    END AS final_cost,
    
    (
        CASE 
            WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
                 AND OI.SubItemSellPrice <> 0 AND OI.SubItemQty <> 0
                THEN OI.SubItemSellPrice * OI.SubItemQty
            ELSE OI.ParentSellPrice * OI.ParentQty
        END
    ) AS TotalFinalPrice,
    
    (
        CASE 
            WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
                 AND OI.SubItemUnitCost <> 0 AND OI.SubItemQty <> 0
                THEN OI.SubItemUnitCost * OI.SubItemQty
            ELSE OI.ParentUnitCost * OI.ParentQty
        END
    ) AS TotalFinalCost,
    
    O.dProcessedOn AS final_date,
    
    CASE
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '' 
            THEN OI.ParentSKU
        ELSE NULL
    END AS kitsku,
    
    'linnworks' AS source,
    OI.ParentTitle,
    OI.OrderId,
    OI.LocationId
FROM [linnworks].[staging].[processed_orders] O
JOIN [linnworks].[lw].[OrderItem_full] OI
    ON O.pkOrderId = OI.OrderId;