INSERT INTO lw.Order_AdditionalInfo (
    fkOrderId,
    BillingName,
    BillingCompany,
    BillingAddress1,
    BillingAddress2,
    BillingAddress3,
    BillingTown,
    BillingRegion,
    BillingPostcode,
    BillingCountryName,
    BillingPhoneNumber,
    BillingEmailAddress,
    dPaidOn, 
    DeliveryStartDate,
    DeliveryEndDate
)
SELECT
    TRY_CAST(data.pkOrderID AS UNIQUEIDENTIFIER) AS fkOrderId,
    data.cFullName AS BillingName,
    data.Company AS BillingCompany,
    data.Address1 AS BillingAddress1,
    data.Address2 AS BillingAddress2,
    data.Address3 AS BillingAddress3,
    data.Town AS BillingTown,
    data.Region AS BillingRegion,
    data.cPostCode AS BillingPostcode,
    data.cCountry AS BillingCountryName,
    data.BuyerPhoneNumber AS BillingPhoneNumber,
    data.cEmailAddress AS BillingEmailAddress,
    TRY_CAST(data.dPaidOn AS DATETIME) AS dPaidOn,
    NULL AS DeliveryStartDate,     -- Column doesn't exist; set as NULL
    NULL AS DeliveryEndDate        -- Column doesn't exist; set as NULL
FROM 
    [linnworks].[staging]._airbyte_raw_processed_orders AS data
WHERE NOT EXISTS (
    SELECT 1 
    FROM lw.Order_AdditionalInfo o
    WHERE o.fkOrderId = TRY_CAST(data.pkOrderID AS UNIQUEIDENTIFIER)
);
