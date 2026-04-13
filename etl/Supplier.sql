WITH RawSuppliers AS (
    SELECT 
        TRY_CAST(JSON_VALUE(supplierData.value, '$.SupplierID') AS UNIQUEIDENTIFIER) AS pkSupplierId,
        JSON_VALUE(supplierData.value, '$.Supplier') AS SupplierName,
        JSON_VALUE(supplierData.value, '$.SupplierCurrency') AS Currency,
        supplierRef.PropertyValue AS ContactName
    FROM [linnworks].[staging]._airbyte_raw_stock_items AS data
    CROSS APPLY OPENJSON(CAST(data.Suppliers AS NVARCHAR(MAX))) AS supplierData
    OUTER APPLY (
        SELECT TOP 1 JSON_VALUE(prop.value, '$.PropertyValue') AS PropertyValue
        FROM OPENJSON(data.ItemExtendedProperties) AS prop
        WHERE JSON_VALUE(prop.value, '$.ProperyName') = 'supplier_ref'
    ) AS supplierRef
),
Ranked AS (
    SELECT 
        rs.*,
        ROW_NUMBER() OVER (
            PARTITION BY pkSupplierId
            ORDER BY 
                CASE WHEN rs.ContactName IS NOT NULL THEN 0 ELSE 1 END
        ) AS RowNum
    FROM RawSuppliers rs
)
INSERT INTO lw.Supplier (
    pkSupplierId,
    SupplierName,
    Currency,
    ContactName
)
SELECT 
    pkSupplierId,
    SupplierName,
    Currency,
    ContactName
FROM Ranked
WHERE RowNum = 1;