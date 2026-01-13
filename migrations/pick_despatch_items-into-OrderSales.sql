;WITH corder AS (
    SELECT DISTINCT
        CAST([Sales_Order_Num] AS VARCHAR(50)) AS [OrderKey], 
        [Order Date] AS [OrderDate]
    FROM [MaginusOMS].[dbo].[fct_Sales_Orders]
)
INSERT INTO [linnworks].[lw].[Order_sales] (
    OrderDate, ItemID, UnitCost, TotalCost, UnitPrice, TotalPrice, 
    UnitTax, TotalTax, TotalIncTax, Quantity, SubItemSKU, 
    SubItemUnitCost, SubItemQty, fkOrderId, FkLocationId, 
    ItemSource, fkStockItemId, [Source], DispatchDate
)
SELECT
    fso.[OrderDate],                                                         
    dp.Product_Code,                                                         
    CASE WHEN fs.Quantity <> 0 THEN ABS(fs.[Total Cost]) / fs.Quantity ELSE 0 END, 
    ABS(fs.[Total Cost]),                                                    
    CASE WHEN fs.Quantity <> 0 THEN fs.[Base Line Value] / fs.Quantity ELSE 0 END, 
    fs.[Base Line Value],                                                    
    CASE WHEN fs.Quantity <> 0 THEN fs.[Base Line VAT Value] / fs.Quantity ELSE 0 END, 
    fs.[Base Line VAT Value],                                                
    fs.[Base Gross Line Value],                                              
    fs.[Quantity],                                                           
    NULL,                                                                    
    NULL,                                                                    
    NULL,                                                                    
    fs.Sales_Order_No,                                                       
    fs.WarehouseKey,                                                         
    NULL,                                                                    
    NULL,                                                                    
    'maginus',                                                               
    TRY_CONVERT(DATETIME, CAST(fs.DateKey AS CHAR(8)), 112)                  
FROM [MaginusOMS].[dbo].[fct_Sales] fs
LEFT JOIN corder fso ON CAST(fs.Sales_Order_No AS VARCHAR(50)) = fso.OrderKey
INNER JOIN [MaginusOMS].[dbo].[Dim_Product] dp ON fs.ProductKey = dp.ProductKey;
GO