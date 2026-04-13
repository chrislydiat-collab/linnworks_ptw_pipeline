INSERT INTO [linnworks].[lw].[purchasing] (
    fkSupplierID,
    contactname,
    suppliername,
    DateOfPurchase,
    ExternalInvoiceNumber,
    SKU,
    UNITCOST,
    Required_Qty,
    Received_Qty,
    TotalCost,
    Due_In_Quantity,
    Status,
    DateOfDelivery,
    Qty,
    source
)
SELECT DISTINCT
    po.fkSupplierID,
    s.contactname,
    s.suppliername,
    fpo.PurchaseOrderHeader_DateOfPurchase,
    fpo.PurchaseOrderHeader_ExternalInvoiceNumber,
    fpo.PurchaseOrderItem_SKU,
    CAST(fpo.DeliveredRecords_UnitCost AS DECIMAL(18,2)) AS UNITCOST,
    CAST(fpo.PurchaseOrderItem_Quantity AS INT) 
        - CAST(fpo.PurchaseOrderItem_Delivered AS INT) AS Required_Qty,
    CAST(fpo.PurchaseOrderItem_Delivered AS INT) AS Received_Qty,
    CAST(fpo.PurchaseOrderItem_cost AS DECIMAL(18,2)) 
        - CAST(fpo.PurchaseOrderItem_tax AS DECIMAL(18,2)) AS TotalCost,
    CAST(fpo.PurchaseOrderItem_Quantity AS INT) 
        - CAST(fpo.PurchaseOrderItem_Delivered AS INT) AS Due_In_Quantity,
    fpo.PurchaseOrderHeader_Status AS Status,
    fpo.PurchaseOrderHeader_DateOfDelivery AS DateOfDelivery,
    CAST(fpo.PurchaseOrderItem_Quantity AS INT) AS Qty,
    'linnworks' AS source
FROM [linnworks].[staging].[FullPurchaseOrders] fpo
JOIN [linnworks].[lw].[PurchaseOrders] po
    ON fpo.PurchaseOrderHeader_pkPurchaseID = po.pkPurchaseID
JOIN [linnworks].[lw].[Supplier] s
    ON po.fkSupplierID = s.pkSupplierId;
