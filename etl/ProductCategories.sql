INSERT INTO lw.ProductCategories (
    CategoryId,
    CategoryName
)
SELECT DISTINCT
    TRY_CAST(CategoryId AS uniqueidentifier) AS CategoryId,
    CategoryName
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items]
WHERE 
    CategoryId IS NOT NULL
    AND CategoryName IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM lw.ProductCategories pc 
        WHERE pc.CategoryId = TRY_CAST([_airbyte_raw_stock_items].CategoryId AS uniqueidentifier)
    );
