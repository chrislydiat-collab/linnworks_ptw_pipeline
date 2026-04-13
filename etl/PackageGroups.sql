INSERT INTO lw.PackageGroups (
    PackageCategoryId,
    PackageCategory,
    bLogicalDelete,
    rowguid,
    PreferenceIndex
)
SELECT
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), '$.PackageCategoryId') AS UNIQUEIDENTIFIER) AS PackageCategoryId,
    JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), '$.PackageCategory') AS PackageCategory,
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), '$.bLogicalDelete') AS BIT) AS bLogicalDelete,
    TRY_CAST(ItemData.RowId AS UNIQUEIDENTIFIER) AS rowguid,
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), '$.PreferenceIndex') AS INT) AS PreferenceIndex
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] AS s
CROSS APPLY OPENJSON(CAST(s.Items AS nvarchar(max))) WITH (
    RowId NVARCHAR(100) '$.RowId'
) AS ItemData;
