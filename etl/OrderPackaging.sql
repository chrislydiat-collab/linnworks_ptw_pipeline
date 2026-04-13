INSERT INTO lw.OrderPackaging (
    fkOrderId, 
    ItemWeight, 
    fkPackagingTypeId, 
    PackagingWeight, 
    TotalWeight, 
    CalcError,  
    ManualAdjust, 
    ErrorState, 
    LabelId, 
    fkPostageFileId, 
    TotalHeight, 
    TotalWidth, 
    TotalDepth
)
SELECT
    TRY_CAST(d.OrderId AS UNIQUEIDENTIFIER) AS fkOrderId,
    TRY_CAST(shipping.ItemWeight AS FLOAT) AS ItemWeight,
    TRY_CAST(shipping.PackageTypeId AS UNIQUEIDENTIFIER) AS fkPackagingTypeId,
    TRY_CAST(shipping.ItemWeight AS FLOAT) AS PackagingWeight,
    TRY_CAST(shipping.TotalWeight AS FLOAT) AS TotalWeight,
    general.LabelError AS CalcError,  -- string, no cast to BIT
    TRY_CAST(shipping.ManualAdjust AS BIT) AS ManualAdjust,
    general.Status AS ErrorState,  
    general.ReferenceNum AS LabelId,
    TRY_CAST(shipping.PostalServiceId AS UNIQUEIDENTIFIER) AS fkPostageFileId,
    s.Height,
    s.Width,
    s.Depth
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details d
CROSS APPLY 
    OPENJSON(d.ShippingInfo) WITH (
        ItemWeight FLOAT '$.ItemWeight',
        PackageTypeId UNIQUEIDENTIFIER '$.PackageTypeId',
        TotalWeight FLOAT '$.TotalWeight',
        ManualAdjust BIT '$.ManualAdjust',
        PostalServiceId UNIQUEIDENTIFIER '$.PostalServiceId'
    ) AS shipping
CROSS APPLY
    OPENJSON(d.GeneralInfo) WITH (
        LabelError NVARCHAR(50) '$.LabelError',
        Status NVARCHAR(50) '$.Status',
        ReferenceNum NVARCHAR(100) '$.ReferenceNum'
    ) AS general
CROSS APPLY
    OPENJSON(d.Items) WITH (
        StockItemId UNIQUEIDENTIFIER '$.StockItemId'
    ) AS items
JOIN 
    [linnworks].[staging]._airbyte_raw_stock_items s
    ON items.StockItemId = s.StockItemId
WHERE 
    d.OrderId IS NOT NULL
    AND items.StockItemId IS NOT NULL;