INSERT INTO lw.PostalServices (
    pkPostalServiceId,  
    PostalServiceName,  
    ServiceCountry,  
    PostalServiceCode,  
    Vendor,  
    TrackingNumberRequired,  
    WeightRequired,  
    rowguid  
)
SELECT
    TRY_CAST(JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), '$.PostalServiceId') AS UNIQUEIDENTIFIER) AS pkPostalServiceId,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), '$.PostalServiceName') AS PostalServiceName,
    o.cCountry AS ServiceCountry,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), '$.PostalServiceCode') AS PostalServiceCode,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), '$.Vendor') AS Vendor,
    o.PostalTrackingNumber AS TrackingNumberRequired,
    o.ItemWeight AS WeightRequired,
    TRY_CAST(JSON_VALUE(CAST(d.Items AS NVARCHAR(MAX)), '$[0].RowId') AS UNIQUEIDENTIFIER) AS rowguid
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details AS d
JOIN 
    [linnworks].[staging]._airbyte_raw_processed_orders AS o
    ON d.OrderId = o.pkOrderID
WHERE 
    d.OrderId IS NOT NULL
    AND o.pkOrderID IS NOT NULL;
