TRUNCATE TABLE [linnworks].[lw].[OrderItem_full];

;WITH ParentItems AS (
    SELECT
        prodortest.SubSource,
        parent.OrderId,
        parent.ItemId AS ParentItemId,
        parent.Title AS ParentTitle,
        parent.SKU AS ParentSKU,
        parent.Quantity AS ParentQty,
        TRY_CONVERT(DECIMAL(18,4), parent.DespatchStockUnitCost) AS ParentUnitCost,
        TRY_CONVERT(DECIMAL(18,4), parent.PricePerUnit) 
            - (TRY_CONVERT(DECIMAL(18,4), parent.DiscountValue) / NULLIF(parent.Quantity, 0)) AS PricePerUnit,
        TRY_CONVERT(DECIMAL(18,4), parent.Tax) AS ParentTax,
        TRY_CONVERT(DECIMAL(5,2), parent.TaxRate) AS ParentTaxRate,
        TRY_CONVERT(DECIMAL(18,4), parent.CostIncTax) AS ParentTotalIncTax,
        TRY_CONVERT(DECIMAL(18,4), parent.Weight) AS ParentWeight,
        br.BinRack AS ParentBinRack,
        br.Location AS LocationId,
        parent.CompositeSubItems,
        parent.ItemSource AS ParentItemSource,
        parent.StockItemId AS ParentStockItemId
    FROM [linnworks].[staging].[_airbyte_raw_processed_order_details] t
    CROSS APPLY OPENJSON(t.GeneralInfo)
        WITH (
            SubSource NVARCHAR(255)
        ) AS prodortest
    CROSS APPLY OPENJSON(t.Items)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            DespatchStockUnitCost NVARCHAR(50),
            PricePerUnit NVARCHAR(50),
            DiscountValue NVARCHAR(50),
            Tax NVARCHAR(50),
            TaxRate NVARCHAR(50),
            CostIncTax NVARCHAR(50),
            Weight NVARCHAR(50),
            BinRacks NVARCHAR(MAX) AS JSON,
            CompositeSubItems NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS parent
    OUTER APPLY OPENJSON(parent.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS br
),
SubItems AS (
    SELECT
    	p.SubSource,
    	p.OrderId,
        p.ParentItemId,
        p.ParentTitle,
        p.ParentSKU,
        p.ParentQty,
        p.ParentUnitCost,
        p.PricePerUnit AS ParentSellPrice,
        p.ParentTax,
        p.ParentTaxRate,
        p.ParentTotalIncTax,
        p.ParentWeight,
        p.ParentBinRack,
        p.LocationId,
        p.ParentItemSource,
        p.ParentStockItemId,
        sub.ItemId,
        sub.Title,
        sub.SKU,
        sub.Quantity,
        TRY_CONVERT(DECIMAL(18,4), sub.UnitCost) AS UnitCost,
        TRY_CONVERT(DECIMAL(18,4), sub.PricePerUnit)
            - (TRY_CONVERT(DECIMAL(18,4), sub.DiscountValue) / NULLIF(sub.Quantity, 0)) AS PricePerUnit,
        TRY_CONVERT(DECIMAL(18,4), sub.Tax) AS Tax,
        TRY_CONVERT(DECIMAL(5,2), sub.TaxRate) AS TaxRate,
        TRY_CONVERT(DECIMAL(18,4), sub.CostIncTax) AS CostIncTax,
        TRY_CONVERT(DECIMAL(18,4), sub.Weight) AS Weight,
        sbr.BinRack,
        sub.ItemSource,
        sub.StockItemId
    FROM ParentItems p
    OUTER APPLY OPENJSON(p.CompositeSubItems)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            UnitCost NVARCHAR(50),
            PricePerUnit NVARCHAR(50),
            DiscountValue NVARCHAR(50),
            Tax NVARCHAR(50),
            TaxRate NVARCHAR(50),
            CostIncTax NVARCHAR(50),
            Weight NVARCHAR(50),
            BinRacks NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS sub
    OUTER APPLY OPENJSON(sub.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS sbr
)
INSERT INTO [linnworks].[lw].[OrderItem_full] (
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    SubItemId,
    SubItemTitle,
    SubItemSKU,
    SubItemQty,
    SubItemUnitCost,
    SubItemSellPrice,
    SubItemTax,
    SubItemTaxRate,
    SubItemTotalIncTax,
    SubItemWeight,
    SubItemBinRack,
    SubItemItemSource,
    SubItemStockItemId
)
SELECT
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    ItemId AS SubItemId,
    Title AS SubItemTitle,
    SKU AS SubItemSKU,
    Quantity AS SubItemQty,
    UnitCost AS SubItemUnitCost,
    PricePerUnit AS SubItemSellPrice,
    Tax AS SubItemTax,
    TaxRate AS SubItemTaxRate,
    CostIncTax AS SubItemTotalIncTax,
    Weight AS SubItemWeight,
    BinRack AS SubItemBinRack,
    ItemSource AS SubItemItemSource,
    StockItemId AS SubItemStockItemId
FROM SubItems

WHERE SubSource NOT IN ('Staging', 'testorder', 'RMA', 'StagingCF');
