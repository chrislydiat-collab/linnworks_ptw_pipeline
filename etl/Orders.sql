DELETE FROM [linnworks].[lw].[Orders] WHERE Source <> 'Maginus';
;WITH awrp AS (
    SELECT 
        balance, 
        CONCAT('MAGENTO', RIGHT(CAST(comment_to_customer AS NVARCHAR(MAX)), 10)) AS linky
    FROM [linnworks].[staging].[aw_rp_transaction]
    WHERE balance < 0
)
INSERT INTO [linnworks].[lw].Orders (
    pkOrderId, cFullName, cEmailAddress, cPostcode, dReceivedDate, dDispatchBy, dProcessedOn, 
    fPostageCost, fTotalCharge, RPDiscount, cCurrency, nOrderId, bReplace, Source, 
    bProcessed, fTax, fkCountryId, fkPostalServiceId, fkPackagingGroupId, ReferenceNum, 
    ExternalReference, PostalTrackingNumber, CreateOnly, CreatedDate, Address1, Address2, 
    Address3, Town, Region, LifeStatus, BuyersPhoneNumber, Company, SubSource, 
    AddressVerified, Subtotal, PostageCostExTax, CountryTaxRate, RecalculateTaxRequired, 
    ChannelBuyerName, HoldOrCancel, Weight, TotalDiscount, fkBankId, FulfillmentLocationId, 
    SecondaryReferenceNum, PostalServiceCost, FulfillmentCenterAcknowledge, PostageDiscount, ConversionRate
)
SELECT
    TRY_CAST(data2.OrderId AS uniqueidentifier),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.ChannelBuyerName'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.EmailAddress'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.PostCode'),
    TRY_CAST(data3.dReceivedDate AS DATETIME),
    TRY_CAST(JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.DespatchByDate') AS DATETIME),
    TRY_CAST(data3.dProcessedOn AS DATETIME),
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.PostageCost') AS DECIMAL(18,2)),
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.TotalCharge') AS DECIMAL(18,2)),
    CAST(-CAST(rp.balance AS DECIMAL(18,2)) / 50 AS DECIMAL(18,2)),
    JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.Currency'),
    data2.NumOrderId,
    NULL, -- bReplace
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.Source'),
    NULL, -- bProcessed
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.Tax') AS DECIMAL(18,2)),
    NULL, NULL, NULL, -- IDs
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.ReferenceNum'),
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.ExternalReferenceNum'),
    JSON_VALUE(CAST(data2.ShippingInfo AS NVARCHAR(MAX)), '$.TrackingNumber'),
    NULL, NULL, -- CreateOnly, CreatedDate
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Address1'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Address2'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Address3'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Town'),
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Region'),
    NULL, NULL, -- LifeStatus, Phone
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.Address.Company'),
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.SubSource'),
    NULL, -- AddressVerified
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.Subtotal') AS DECIMAL(18,2)),
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.PostageCostExTax') AS DECIMAL(18,2)),
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.CountryTaxRate') AS DECIMAL(18,2)),
    NULL, -- RecalculateTax
    JSON_VALUE(CAST(data2.CustomerInfo AS NVARCHAR(MAX)), '$.ChannelBuyerName'),
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.HoldOrCancel'),
    NULL, -- Weight
    CAST(JSON_VALUE(CAST(data2.TotalsInfo AS NVARCHAR(MAX)), '$.TotalDiscount') AS DECIMAL(18,2)), -- CASTED THIS
    NULL, -- fkBankId
    data2.FulfilmentLocationId,
    NULL, NULL, NULL, NULL, NULL -- Last 5 columns
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] AS data2
    LEFT JOIN [linnworks].[staging].[processed_orders] AS data3 ON data2.OrderId = data3.pkOrderID
    LEFT JOIN awrp AS rp ON rp.linky = JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.ReferenceNum')
WHERE
    JSON_VALUE(CAST(data2.GeneralInfo AS NVARCHAR(MAX)), '$.SubSource') NOT IN ('Staging', 'testorder', 'RMA', 'StagingCF');