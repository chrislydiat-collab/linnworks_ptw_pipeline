-- 1. Clear existing data
DELETE FROM [linnworks].[lw].[final_orderitems]
WHERE [source] = 'maginus';
;WITH bodge AS (
    SELECT 
        fs.Despatch_Num,
        MIN(fso.[Order Date]) AS order_date,
        MAX(fs.Despatch_Date) AS Despatch_Date
    FROM [MaginusOMS].[dbo].[fct_Sales] fs
    left join [MaginusOMS].[dbo].[fct_Sales_Orders] fso ON fso.Sales_Order_Num = fs.Sales_Order_No
    GROUP BY Despatch_Num
)
INSERT INTO [linnworks].[lw].[final_orderitems] (
    final_sku,
    final_quantity,
    final_price,
    final_cost,
    DiscountPerUnit,
    TotalFinalPrice,
    TotalFinalCost,
    TotalDiscount,
    order_date,
    final_date,
    kitsku,
    source,
    Title,
    OrderId,
    LocationId
)
SELECT
    pdi.PRODUCT_CODE AS final_sku,
    pdi.ACTUAL_QUANTITY AS final_quantity,
    (CASE 
         WHEN pdi.ACTUAL_QUANTITY = 0 THEN 0 
         ELSE (pdi.UNIT_PRICE - (pdi.VAT_AMOUNT / pdi.ACTUAL_QUANTITY)) * (100 - pdi.DISCOUNT_PERC) / 100
     END) AS final_price,
    pdi.UNIT_COST AS final_cost,
    (CASE 
         WHEN pdi.ACTUAL_QUANTITY = 0 THEN 0 
         ELSE (pdi.UNIT_PRICE - (pdi.VAT_AMOUNT / pdi.ACTUAL_QUANTITY)) * (pdi.DISCOUNT_PERC) / 100
     END) AS DiscountPerUnit,
    ((pdi.UNIT_PRICE * pdi.ACTUAL_QUANTITY) - pdi.VAT_AMOUNT) * (100 - pdi.DISCOUNT_PERC) / 100 AS TotalFinalPrice,
    (pdi.UNIT_COST * pdi.ACTUAL_QUANTITY) AS TotalFinalCost,
    ((pdi.UNIT_PRICE * pdi.ACTUAL_QUANTITY) - pdi.VAT_AMOUNT) * (pdi.DISCOUNT_PERC) / 100 AS TotalDiscount,
    bodge.order_date AS order_date,
    (CASE 
	    WHEN CAST(bodge.order_date AS DATE) > '2023-11-24' AND bodge.order_date > bodge.Despatch_Date
	    	THEN bodge.order_date -- fixing 12 cases of data errors in maginus
	    ELSE bodge.Despatch_Date
    END) AS final_date,
    pdi.KIT_PRODUCT_CODE AS kitsku,
    'maginus' AS source,
    NULL AS Title,
    pdi.SALES_DOCUMENT_NUM AS OrderId,
    pdi.WarehouseKey AS LocationId
FROM [MaginusOMS].[dbo].[PICK_DESPATCH_ITEM] pdi
LEFT JOIN bodge ON pdi.DESPATCH_NUM = bodge.Despatch_Num
WHERE bodge.order_date >= '2020-01-01';