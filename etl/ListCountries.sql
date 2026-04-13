INSERT INTO lw.ListCountries (
    pkCountryId,  
    cCountry,  
    cCurrency,  
    nPostageCostPerKg,  
    rowguid,  
    TaxRate  
)
SELECT
    TRY_CAST(JSON_VALUE(CAST(d.CustomerInfo AS NVARCHAR(MAX)), '$.Address.CountryId') AS UNIQUEIDENTIFIER) AS pkCountryId,
    o.cCountry,
    o.cCurrency,
    o.fPostageCost AS nPostageCostPerKg,
    TRY_CAST(item.RowId AS UNIQUEIDENTIFIER) AS rowguid,
    TRY_CAST(item.TaxRate AS FLOAT) AS TaxRate
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] d
JOIN 
    [linnworks].[staging].[_airbyte_raw_processed_orders] o
    ON d.OrderId = o.pkOrderID
CROSS APPLY OPENJSON(CAST(d.Items AS NVARCHAR(MAX)))
WITH (
    RowId UNIQUEIDENTIFIER,
    TaxRate FLOAT
) AS item
WHERE 
    d.CustomerInfo IS NOT NULL
    AND d.Items IS NOT NULL;