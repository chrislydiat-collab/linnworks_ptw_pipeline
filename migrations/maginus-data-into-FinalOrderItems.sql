;WITH fs AS (
    SELECT 
        Despatch_Num,
        MAX(Despatch_Date) AS Despatch_Date
    FROM [MaginusOMS].[dbo].[fct_Sales]
    GROUP BY Despatch_Num
)
INSERT INTO [linnworks].[lw].[final_orderitems] (
    final_sku,
    final_quantity,
    final_price,
    final_cost,
    TotalFinalPrice,
    TotalFinalCost,
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
         ELSE (pdi.UNIT_PRICE - (pdi.VAT_AMOUNT / pdi.ACTUAL_QUANTITY))
     END) AS final_price,
    pdi.UNIT_COST AS final_cost,
    (pdi.UNIT_PRICE * pdi.ACTUAL_QUANTITY) - pdi.VAT_AMOUNT AS TotalFinalPrice,
    (pdi.UNIT_COST * pdi.ACTUAL_QUANTITY) AS TotalFinalCost,
    fs.Despatch_Date AS final_date,
    pdi.KIT_PRODUCT_CODE AS kitsku,
    'maginus' AS source,
    NULL AS Title,
    pdi.SALES_DOCUMENT_NUM AS OrderId,
    pdi.WarehouseKey AS LocationId
FROM [MaginusOMS].[dbo].[PICK_DESPATCH_ITEM] pdi
LEFT JOIN fs ON pdi.DESPATCH_NUM = fs.Despatch_Num;
