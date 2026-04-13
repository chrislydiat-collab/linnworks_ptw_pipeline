INSERT INTO lw.ItemSupplier (
    fkStockItemId,
    fkSupplierId,
    rowid,
    IsDefault,
    SupplierCode,
    SupplierCode2,
    SupplierBarcode,
    LeadTime,
    KnownPurchasePrice,
    AvgPurchasePrice,
    AvgLeadTime,
    MaxLeadTime,
    MinOrder,
    OnHand,
    MinPurchasePrice,
    MaxPurchasePrice,
    AvgPurchaseQty,
    SupplierMinOrderQty,
    SupplierPackSize,
    LeadTimeVector
)
SELECT 
    TRY_CAST(supplier.StockItemId AS UNIQUEIDENTIFIER) AS fkStockItemId,
    TRY_CAST(supplier.SupplierID AS UNIQUEIDENTIFIER) AS fkSupplierId,
    NEWID() AS rowid,  -- replace with real if available
    supplier.IsDefault,
    supplier.Code AS SupplierCode,
    supplier.Code AS SupplierCode2,
    supplier.SupplierBarcode,
    TRY_CAST(supplier.LeadTime AS INT) AS LeadTime,
    TRY_CAST(supplier.PurchasePrice AS DECIMAL(18, 2)) AS KnownPurchasePrice,
    TRY_CAST(supplier.AveragePrice AS DECIMAL(18, 2)) AS AvgPurchasePrice,
    TRY_CAST(supplier.AverageLeadTime AS INT) AS AvgLeadTime,
    TRY_CAST(supplier.MaxLeadTime AS INT) AS MaxLeadTime,
    TRY_CAST(supplier.SupplierMinOrderQty AS INT) AS MinOrder,
    TRY_CAST(supplier.OnHand AS INT) AS OnHand,
    TRY_CAST(supplier.MinPrice AS DECIMAL(18, 2)) AS MinPurchasePrice,
    TRY_CAST(supplier.MaxPrice AS DECIMAL(18, 2)) AS MaxPurchasePrice,
    TRY_CAST(supplier.AvgPurchaseQty AS INT) AS AvgPurchaseQty,
    TRY_CAST(supplier.SupplierMinOrderQty AS INT) AS SupplierMinOrderQty,
    TRY_CAST(supplier.SupplierPackSize AS INT) AS SupplierPackSize,
    supplier.LeadTimeVector
FROM 
    [linnworks].[staging]._airbyte_raw_stock_items AS data
CROSS APPLY 
    OPENJSON(CAST(data.Suppliers AS NVARCHAR(MAX)))
    WITH (
        StockItemId UNIQUEIDENTIFIER '$.StockItemId',
        SupplierID UNIQUEIDENTIFIER '$.SupplierID',
        IsDefault BIT '$.IsDefault',
        Code NVARCHAR(100) '$.Code',
        SupplierBarcode NVARCHAR(100) '$.SupplierBarcode',
        LeadTime INT '$.LeadTime',
        PurchasePrice DECIMAL(18,2) '$.PurchasePrice',
        AveragePrice DECIMAL(18,2) '$.AveragePrice',
        AverageLeadTime INT '$.AverageLeadTime',
        MaxLeadTime INT '$.MaxLeadTime',
        SupplierMinOrderQty INT '$.SupplierMinOrderQty',
        OnHand INT '$.OnHand',
        MinPrice DECIMAL(18,2) '$.MinPrice',
        MaxPrice DECIMAL(18,2) '$.MaxPrice',
        AvgPurchaseQty INT '$.AvgPurchaseQty',
        SupplierPackSize INT '$.SupplierPackSize',
        LeadTimeVector NVARCHAR(255) '$.LeadTimeVector'
    ) AS supplier;
