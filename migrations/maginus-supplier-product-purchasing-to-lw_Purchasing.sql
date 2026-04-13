INSERT INTO [linnworks].[lw].[purchasing] (
    fksupplierID,
    ContactName,
    suppliername,
    DateOfPurchase,
    ExternalInvoiceNumber,
    SKU,
    UnitCost,
    Required_Qty,
    Received_Qty,
    TotalCost,
    Due_In_Quantity,
    Status,
    DateOfDelivery,
    Qty,
    source
)
SELECT
    sm.NewSupplierGUID AS fksupplierID,  -- mapped GUID
    ds.supplier_account AS ContactName,
    ds.[Supplier Name] AS suppliername,
    fp.order_date AS DateOfPurchase,
    fp.order_number AS ExternalInvoiceNumber,
    dp.product_code AS SKU,
    fp.po_net_price AS UnitCost,
    fp.PO_Required_qty AS Required_Qty,
    fp.PO_Received_Qty AS Received_Qty,
    fp.PO_Base_ORder_Value AS TotalCost,
    fp.Due_In_Quantity,
    fp.Purchase_Line_Status AS Status,
    fp.GRN_Receipt_Date AS DateOfDelivery,
    fp.Due_In_Quantity + fp.PO_Received_Qty AS Qty,
    'maginus' AS source
FROM [MaginusOMS].[dbo].[fct_Purchasing] fp
INNER JOIN [MaginusOMS].[dbo].[Dim_Supplier] ds
    ON fp.SupplierKey = ds.SupplierKey
INNER JOIN [MaginusOMS].[dbo].[Dim_Product] dp
    ON fp.ProductKey = dp.ProductKey
INNER JOIN [linnworks].[staging].[maginus_SupplierID_Mapping] sm
    ON ds.SupplierKey = sm.OldSupplierID;