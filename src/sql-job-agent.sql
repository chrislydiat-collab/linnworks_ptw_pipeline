USE [msdb]
GO

/****** Object:  Job [airbyte_etl]     ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]     ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'airbyte_etl', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sqlserver', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [ItemLocation]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ItemLocation', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM lw.ItemLocation;

INSERT INTO lw.ItemLocation (
    fkStockItemId, 
    fkLocationId, 
    BinRackNumber, 
    rowid
)
SELECT
    JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), ''$[0].StockItemId'') AS fkStockItemId,
    d.[FulfilmentLocationId] AS fkLocationId,
    COALESCE(
        NULLIF(JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), ''$[0].BinRack''), ''''),
        JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), ''$[0].BinRacks[1].BinRack'')
    ) AS BinRackNumber,
    JSON_VALUE(CAST(d.[Items] AS NVARCHAR(MAX)), ''$[0].RowId'') AS rowid
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] d;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [orders]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'orders', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO lw.Orders (
    pkOrderId,  
    cFullName,
    cEmailAddress,
    cPostcode,
    dReceivedDate,
    dProcessedOn,
    fPostageCost,
    fTotalCharge,
    cCurrency,
    nOrderId,
    bReplace,
    Source,
    bProcessed,
    fTax,
    fkCountryId,
    fkPostalServiceId,
    fkPackagingGroupId,
    ReferenceNum,
    ExternalReference,
    PostalTrackingNumber,
    CreateOnly,
    CreatedDate,
    Address1,
    Address2,
    Address3,
    Town,
    Region,
    LifeStatus,
    BuyersPhoneNumber,
    Company,
    SubSource,
    AddressVerified,
    Subtotal,
    PostageCostExTax,
    CountryTaxRate,
    RecalculateTaxRequired,
    ChannelBuyerName,
    HoldOrCancel,
    Weight,
    TotalDiscount,
    fkBankId,
    FulfillmentLocationId,
    SecondaryReferenceNum,
    PostalServiceCost,
    FulfillmentCenterAcknowledge,
    PostageDiscount,
    ConversionRate
)
SELECT
    TRY_CAST(pkOrderID AS uniqueidentifier),
    cFullName,
    cEmailAddress,
    REPLACE(cPostCode, CHAR(9), '''') AS cPostcode,
    TRY_CAST(dReceivedDate AS DATETIME),
    TRY_CAST(dProcessedOn AS DATETIME),
    fPostageCost,
    fTotalCharge,
    cCurrency,
    nOrderId,
    NULL AS bReplace,
    Source,
    NULL AS bProcessed,
    fTax,
    NULL AS fkCountryId,
    NULL AS fkPostalServiceId,
    NULL AS fkPackagingGroupId,
    ReferenceNum,
    ExternalReference,
    PostalTrackingNumber,
    NULL AS CreateOnly,
    NULL AS CreatedDate,
    Address1,
    Address2,
    Address3,
    Town,
    Region,
    NULL AS LifeStatus,
    NULL AS BuyersPhoneNumber,
    Company,
    SubSource,
    NULL AS AddressVerified,
    Subtotal,
    PostageCostExTax,
    CountryTaxRate,
    NULL AS RecalculateTaxRequired,
    ChannelBuyerName,
    HoldOrCancel,
    NULL AS Weight,
    TotalDiscount,
    NULL AS fkBankId,
    NULL AS FulfillmentLocationId,
    NULL AS SecondaryReferenceNum,
    NULL AS PostalServiceCost,
    NULL AS FulfillmentCenterAcknowledge,
    NULL AS PostageDiscount,
    NULL AS ConversionRate
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_orders] AS data
WHERE 
    pkOrderID IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM lw.Orders o 
        WHERE o.pkOrderId = TRY_CAST(data.pkOrderID AS uniqueidentifier)
    );
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [ProductCategories]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ProductCategories', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO lw.ProductCategories (
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
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [OrderItem_full]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'OrderItem_full', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'TRUNCATE TABLE [linnworks].[lw].[OrderItem_full];

;WITH ParentItems AS (
    SELECT
        parent.OrderId,
        parent.ItemId AS ParentItemId,
        parent.Title AS ParentTitle,
        parent.SKU AS ParentSKU,
        parent.Quantity AS ParentQty,
        parent.UnitCost AS ParentUnitCost,
        parent.PricePerUnit AS ParentSellPrice,
        parent.Tax AS ParentTax,
        parent.TaxRate AS ParentTaxRate,
        parent.CostIncTax AS ParentTotalIncTax,
        parent.Weight AS ParentWeight,
        br.BinRack AS ParentBinRack,
        br.Location AS LocationId,
        parent.CompositeSubItems,
        parent.ItemSource AS ParentItemSource,
        parent.StockItemId AS ParentStockItemId
    FROM [linnworks].[staging].[_airbyte_raw_processed_order_details] t
    CROSS APPLY OPENJSON(t.Items)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            UnitCost DECIMAL(18,4),
            PricePerUnit DECIMAL(18,4),
            Tax DECIMAL(18,4),
            TaxRate DECIMAL(5,2),
            CostIncTax DECIMAL(18,4),
            Weight DECIMAL(18,4),
            BinRacks NVARCHAR(MAX) AS JSON,
            CompositeSubItems NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS parent
    OUTER APPLY OPENJSON(parent.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS br
),
SubItems AS (
    SELECT
        p.OrderId,
        p.ParentItemId,
        p.ParentTitle,
        p.ParentSKU,
        p.ParentQty,
        p.ParentUnitCost,
        p.ParentSellPrice,
        p.ParentTax,
        p.ParentTaxRate,
        p.ParentTotalIncTax,
        p.ParentWeight,
        p.ParentBinRack,
        p.LocationId,
        p.ParentItemSource,
        p.ParentStockItemId,
        sub.ItemId,
        sub.Title,
        sub.SKU,
        sub.Quantity,
        sub.UnitCost,
        sub.PricePerUnit,
        sub.Tax,
        sub.TaxRate,
        sub.CostIncTax,
        sub.Weight,
        sbr.BinRack,
        sub.ItemSource,
        sub.StockItemId
    FROM ParentItems p
    OUTER APPLY OPENJSON(p.CompositeSubItems)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            UnitCost DECIMAL(18,4),
            PricePerUnit DECIMAL(18,4),
            Tax DECIMAL(18,4),
            TaxRate DECIMAL(5,2),
            CostIncTax DECIMAL(18,4),
            Weight DECIMAL(18,4),
            BinRacks NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS sub
    OUTER APPLY OPENJSON(sub.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS sbr
)
INSERT INTO [linnworks].[lw].[OrderItem_full] (
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    SubItemId,
    SubItemTitle,
    SubItemSKU,
    SubItemQty,
    SubItemUnitCost,
    SubItemSellPrice,
    SubItemTax,
    SubItemTaxRate,
    SubItemTotalIncTax,
    SubItemWeight,
    SubItemBinRack,
    SubItemItemSource,
    SubItemStockItemId
)
SELECT
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    ItemId AS SubItemId,
    Title AS SubItemTitle,
    SKU AS SubItemSKU,
    Quantity AS SubItemQty,
    UnitCost AS SubItemUnitCost,
    PricePerUnit AS SubItemSellPrice,
    Tax AS SubItemTax,
    TaxRate AS SubItemTaxRate,
    CostIncTax AS SubItemTotalIncTax,
    Weight AS SubItemWeight,
    BinRack AS SubItemBinRack,
    ItemSource AS SubItemItemSource,
    StockItemId AS SubItemStockItemId
FROM SubItems;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [StockItems]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'StockItems', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM lw.StockItems;

INSERT INTO lw.StockItems (
    pkStockID,
    cItemNumber,
    cItemName,
    cDescription,
    fPricePerItem,
    fkStockControlStockItemId
)
SELECT 
    TRY_CAST(epDetails.pkRowId AS uniqueidentifier) AS pkStockID,
    s.ItemNumber AS cItemNumber,
    s.ItemTitle AS cItemName,
    s.ItemChannelDescriptions AS cDescription,
    TRY_CAST(s.RetailPrice AS DECIMAL(18,2)) AS fPricePerItem,
    epDetails.fkStockItemId AS fkStockControlStockItemId
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items] s
OUTER APPLY (
    SELECT 
        epValues.pkRowId,
        epValues.fkStockItemId
    FROM 
        OPENJSON(s.ItemExtendedProperties) AS ep
    CROSS APPLY 
        OPENJSON(ep.value) WITH (
            pkRowId uniqueidentifier ''$.pkRowId'',
            fkStockItemId uniqueidentifier ''$.fkStockItemId''
        ) AS epValues
    WHERE ep.[key] = ''0'' OR ep.[key] IS NULL
) AS epDetails
WHERE 
    NOT EXISTS (
        SELECT 1 
        FROM lw.StockItems si
        WHERE si.pkStockID = TRY_CAST(epDetails.pkRowId AS uniqueidentifier)
    );', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [StockItem]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'StockItem', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM [linnworks].[lw].[StockItem]
WHERE Source = ''linnworks'';

INSERT INTO lw.StockItem (
    pkStockItemID,
    ItemTitle,
    ItemNumber,
    ItemDescription,
    CreationDate,
    bLogicalDelete,
    RetailPrice,
    CategoryId,
    Weight,
    PackageGroup,
    BinRack,
    rowguid,
    PurchasePrice,
    BarcodeNumber,
    DimHeight,
    DimWidth,
    DimDepth,
    ShippedSeparately,
    TaxRate,
    fkPostalService,
    bContainsComposites,
    ModifiedDate,
    ModifiedUserName,
    ModifyAction,
    IsArchived,
    IsVariationGroup,
    InventoryTrackingType,
    SerialNumberScanRequired,
    BatchNumberScanRequired,
    Source
)
SELECT 
    TRY_CAST(JSON_VALUE(CAST(s.Suppliers AS nvarchar(max)), ''$[0].StockItemId'') AS uniqueidentifier) AS pkStockItemID,
    s.ItemTitle,
    s.ItemNumber,
    s.ItemChannelDescriptions AS ItemDescription,
    TRY_CAST(s.CreationDate AS DATETIME) AS CreationDate,
    NULL AS bLogicalDelete,
    TRY_CAST(s.RetailPrice AS DECIMAL(18,2)) AS RetailPrice,
    TRY_CAST(s.CategoryId AS uniqueidentifier) AS CategoryId,
    TRY_CAST(s.Weight AS DECIMAL(18,2)) AS Weight,
    s.PackageGroupId AS PackageGroup,
    NULL AS BinRack,
    NULL AS rowguid,
    TRY_CAST(s.PurchasePrice AS DECIMAL(18,2)) AS PurchasePrice,
    s.BarcodeNumber,
    TRY_CAST(s.Height AS DECIMAL(18,2)) AS DimHeight,
    TRY_CAST(s.Width AS DECIMAL(18,2)) AS DimWidth,
    TRY_CAST(s.Depth AS DECIMAL(18,2)) AS DimDepth,
    NULL AS ShippedSeparately,
    TRY_CAST(s.TaxRate AS DECIMAL(18,2)) AS TaxRate,
    s.PostalServiceId AS fkPostalService,
    NULL AS bContainsComposites,
    NULL AS ModifiedDate,
    NULL AS ModifiedUserName,
    NULL AS ModifyAction,
    NULL AS IsArchived,
    NULL AS IsVariationGroup,
    TRY_CAST(s.InventoryTrackingType AS INT) AS InventoryTrackingType,
    TRY_CAST(s.SerialNumberScanRequired AS BIT) AS SerialNumberScanRequired,
    TRY_CAST(s.BatchNumberScanRequired AS BIT) AS BatchNumberScanRequired,
    ''linnworks'' AS Source
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items] s;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [PackageGroups]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'PackageGroups', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
DELETE FROM [linnworks].[lw].[PackageGroups];

INSERT INTO lw.PackageGroups (
    PackageCategoryId,
    PackageCategory,
    bLogicalDelete,
    rowguid,
    PreferenceIndex
)
SELECT
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), ''$.PackageCategoryId'') AS UNIQUEIDENTIFIER) AS PackageCategoryId,
    JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), ''$.PackageCategory'') AS PackageCategory,
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), ''$.bLogicalDelete'') AS BIT) AS bLogicalDelete,
    TRY_CAST(ItemData.RowId AS UNIQUEIDENTIFIER) AS rowguid,
    TRY_CAST(JSON_VALUE(CAST(s.ShippingInfo AS nvarchar(max)), ''$.PreferenceIndex'') AS INT) AS PreferenceIndex
FROM 
    [linnworks].[staging].[_airbyte_raw_processed_order_details] AS s
CROSS APPLY OPENJSON(CAST(s.Items AS nvarchar(max))) WITH (
    RowId NVARCHAR(100) ''$.RowId''
) AS ItemData;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Open_OrderItem]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Open_OrderItem', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO lw.Open_OrderItem (
    Rowid,
    fkOrderId,
    fPricePerUnit,
    nQty,
    ItemNumber,
    fkLocationId,
    fkCompositeParentRowId,
    ChannelSKU,
    fkStockItemId,
    SalesTax,
    TaxRate,
    TaxCostInclusive,
    Cost,
    CostIncTax,
    PartShipped,
    LineDiscount,
    IsService,
    ItemSource,
    PartShippedQty,
    OriginalTitle,
    AffectingStockLevel,
    AvailableStock
)
SELECT
    item.RowId AS Rowid,
    TRY_CAST(data.OrderId AS UNIQUEIDENTIFIER) AS fkOrderId,
    item.PricePerUnit AS fPricePerUnit,
    SUM(BinRackData.Quantity) AS nQty,
    item.ItemNumber AS ItemNumber,
    TRY_CAST(data.FulfilmentLocationId AS UNIQUEIDENTIFIER) AS fkLocationId,
    NULL AS fkCompositeParentRowId,
    item.ChannelSKU AS ChannelSKU,
    item.FkStockItemId AS fkStockItemId,
    item.SalesTax AS SalesTax,
    item.TaxRate AS TaxRate,
    item.TaxCostInclusive AS TaxCostInclusive,
    item.Cost AS Cost,
    item.CostIncTax AS CostIncTax,
    item.PartShipped AS PartShipped,
    item.Discount AS LineDiscount,
    item.IsService AS IsService,
    item.ItemSource AS ItemSource,
    item.PartShippedQty AS PartShippedQty,
    item.Title AS OriginalTitle,
    item.StockLevelIndicator AS AffectingStockLevel,
    item.AvailableStock AS AvailableStock
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details AS data
CROSS APPLY OPENJSON(CAST(data.Items AS nvarchar(max)))
WITH (
    RowId UNIQUEIDENTIFIER,
    ItemNumber NVARCHAR(50),
    ChannelSKU NVARCHAR(50),
    FkStockItemId UNIQUEIDENTIFIER ''$.StockItemId'',
    PricePerUnit FLOAT,
    SalesTax FLOAT,
    TaxRate FLOAT,
    TaxCostInclusive BIT,
    Cost FLOAT,
    CostIncTax FLOAT,
    PartShipped BIT,
    Discount FLOAT,
    IsService BIT,
    ItemSource NVARCHAR(50),
    PartShippedQty INT,
    Title NVARCHAR(255),
    StockLevelIndicator INT,
    AvailableStock INT,
    BinRacks nvarchar(max) AS JSON
) AS item
OUTER APPLY OPENJSON(item.BinRacks)
WITH (
    Quantity INT ''$.Quantity''
) AS BinRackData
WHERE NOT EXISTS (
    SELECT 1 
    FROM lw.Open_OrderItem oi
    WHERE oi.Rowid = item.RowId
)
GROUP BY
    item.RowId,
    data.OrderId,
    item.PricePerUnit,
    item.ItemNumber,
    data.FulfilmentLocationId,
    item.ChannelSKU,
    item.FkStockItemId,
    item.SalesTax,
    item.TaxRate,
    item.TaxCostInclusive,
    item.Cost,
    item.CostIncTax,
    item.PartShipped,
    item.Discount,
    item.IsService,
    item.ItemSource,
    item.PartShippedQty,
    item.Title,
    item.StockLevelIndicator,
    item.AvailableStock;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [PostalServices]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'PostalServices', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete from lw.PostalServices

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
    TRY_CAST(JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), ''$.PostalServiceId'') AS UNIQUEIDENTIFIER) AS pkPostalServiceId,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), ''$.PostalServiceName'') AS PostalServiceName,
    o.cCountry AS ServiceCountry,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), ''$.PostalServiceCode'') AS PostalServiceCode,
    JSON_VALUE(CAST(d.ShippingInfo AS NVARCHAR(MAX)), ''$.Vendor'') AS Vendor,
    o.PostalTrackingNumber AS TrackingNumberRequired,
    o.ItemWeight AS WeightRequired,
    TRY_CAST(JSON_VALUE(CAST(d.Items AS NVARCHAR(MAX)), ''$[0].RowId'') AS UNIQUEIDENTIFIER) AS rowguid
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details AS d
JOIN 
    [linnworks].[staging]._airbyte_raw_processed_orders AS o
    ON d.OrderId = o.pkOrderID
WHERE 
    d.OrderId IS NOT NULL
    AND o.pkOrderID IS NOT NULL;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Order_AdditionalInfo]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Order_AdditionalInfo', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO lw.Order_AdditionalInfo (
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
    NULL AS DeliveryStartDate,     -- Column doesn''t exist; set as NULL
    NULL AS DeliveryEndDate        -- Column doesn''t exist; set as NULL
FROM 
    [linnworks].[staging]._airbyte_raw_processed_orders AS data
WHERE NOT EXISTS (
    SELECT 1 
    FROM lw.Order_AdditionalInfo o
    WHERE o.fkOrderId = TRY_CAST(data.pkOrderID AS UNIQUEIDENTIFIER)
);
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Supplier]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Supplier', 
		@step_id=11, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'WITH RawSuppliers AS (
    SELECT 
        TRY_CAST(JSON_VALUE(supplierData.value, ''$.SupplierID'') AS UNIQUEIDENTIFIER) AS pkSupplierId,
        JSON_VALUE(supplierData.value, ''$.Supplier'') AS SupplierName,
        JSON_VALUE(supplierData.value, ''$.SupplierCurrency'') AS Currency,
        supplierRef.PropertyValue AS ContactName
    FROM [linnworks].[staging]._airbyte_raw_stock_items AS data
    CROSS APPLY OPENJSON(CAST(data.Suppliers AS NVARCHAR(MAX))) AS supplierData
    OUTER APPLY (
        SELECT TOP 1 JSON_VALUE(prop.value, ''$.PropertyValue'') AS PropertyValue
        FROM OPENJSON(data.ItemExtendedProperties) AS prop
        WHERE JSON_VALUE(prop.value, ''$.ProperyName'') = ''supplier_ref''
    ) AS supplierRef
),
Ranked AS (
    SELECT 
        rs.*,
        ROW_NUMBER() OVER (
            PARTITION BY pkSupplierId
            ORDER BY 
                CASE WHEN rs.ContactName IS NOT NULL THEN 0 ELSE 1 END
        ) AS RowNum
    FROM RawSuppliers rs
)
INSERT INTO lw.Supplier (
    pkSupplierId,
    SupplierName,
    Currency,
    ContactName
)
SELECT 
    pkSupplierId,
    SupplierName,
    Currency,
    ContactName
FROM Ranked
WHERE RowNum = 1;', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [ItemSupplier]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ItemSupplier', 
		@step_id=12, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM lw.ItemSupplier;

INSERT INTO lw.ItemSupplier (
    fkStockItemId,
    fkSupplierId,
    rowid,
    IsDefault,
    SupplierCode,
    SupplierCode2,
    SupplierBarcode,
    LeadTime,
    KnownPurchasePrice,
    AvgPurchasePrice,
    AvgLeadTime,
    MaxLeadTime,
    MinOrder,
    OnHand,
    MinPurchasePrice,
    MaxPurchasePrice,
    AvgPurchaseQty,
    SupplierMinOrderQty,
    SupplierPackSize,
    LeadTimeVector
)
SELECT 
    TRY_CAST(supplier.StockItemId AS UNIQUEIDENTIFIER) AS fkStockItemId,
    TRY_CAST(supplier.SupplierID AS UNIQUEIDENTIFIER) AS fkSupplierId,
    NEWID() AS rowid,  -- replace with real if available
    supplier.IsDefault,
    supplier.Code AS SupplierCode,
    supplier.Code AS SupplierCode2,
    supplier.SupplierBarcode,
    TRY_CAST(supplier.LeadTime AS INT) AS LeadTime,
    TRY_CAST(supplier.PurchasePrice AS DECIMAL(18, 2)) AS KnownPurchasePrice,
    TRY_CAST(supplier.AveragePrice AS DECIMAL(18, 2)) AS AvgPurchasePrice,
    TRY_CAST(supplier.AverageLeadTime AS INT) AS AvgLeadTime,
    TRY_CAST(supplier.MaxLeadTime AS INT) AS MaxLeadTime,
    TRY_CAST(supplier.SupplierMinOrderQty AS INT) AS MinOrder,
    TRY_CAST(supplier.OnHand AS INT) AS OnHand,
    TRY_CAST(supplier.MinPrice AS DECIMAL(18, 2)) AS MinPurchasePrice,
    TRY_CAST(supplier.MaxPrice AS DECIMAL(18, 2)) AS MaxPurchasePrice,
    TRY_CAST(supplier.AvgPurchaseQty AS INT) AS AvgPurchaseQty,
    TRY_CAST(supplier.SupplierMinOrderQty AS INT) AS SupplierMinOrderQty,
    TRY_CAST(supplier.SupplierPackSize AS INT) AS SupplierPackSize,
    supplier.LeadTimeVector
FROM 
    [linnworks].[staging]._airbyte_raw_stock_items AS data
CROSS APPLY 
    OPENJSON(CAST(data.Suppliers AS NVARCHAR(MAX)))
    WITH (
        StockItemId UNIQUEIDENTIFIER ''$.StockItemId'',
        SupplierID UNIQUEIDENTIFIER ''$.SupplierID'',
        IsDefault BIT ''$.IsDefault'',
        Code NVARCHAR(100) ''$.Code'',
        SupplierBarcode NVARCHAR(100) ''$.SupplierBarcode'',
        LeadTime INT ''$.LeadTime'',
        PurchasePrice DECIMAL(18,2) ''$.PurchasePrice'',
        AveragePrice DECIMAL(18,2) ''$.AveragePrice'',
        AverageLeadTime INT ''$.AverageLeadTime'',
        MaxLeadTime INT ''$.MaxLeadTime'',
        SupplierMinOrderQty INT ''$.SupplierMinOrderQty'',
        OnHand INT ''$.OnHand'',
        MinPrice DECIMAL(18,2) ''$.MinPrice'',
        MaxPrice DECIMAL(18,2) ''$.MaxPrice'',
        AvgPurchaseQty INT ''$.AvgPurchaseQty'',
        SupplierPackSize INT ''$.SupplierPackSize'',
        LeadTimeVector NVARCHAR(255) ''$.LeadTimeVector''
    ) AS supplier;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [OrderPackaging]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'OrderPackaging', 
		@step_id=13, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete from lw.OrderPackaging
INSERT INTO lw.OrderPackaging (
    fkOrderId, 
    ItemWeight, 
    fkPackagingTypeId, 
    PackagingWeight, 
    TotalWeight, 
    CalcError,  -- now NVARCHAR
    ManualAdjust, 
    ErrorState, 
    LabelId, 
    fkPostageFileId, 
    TotalHeight, 
    TotalWidth, 
    TotalDepth
)
SELECT
    TRY_CAST(d.OrderId AS UNIQUEIDENTIFIER) AS fkOrderId,
    TRY_CAST(shipping.ItemWeight AS FLOAT) AS ItemWeight,
    TRY_CAST(shipping.PackageTypeId AS UNIQUEIDENTIFIER) AS fkPackagingTypeId,
    TRY_CAST(shipping.ItemWeight AS FLOAT) AS PackagingWeight,
    TRY_CAST(shipping.TotalWeight AS FLOAT) AS TotalWeight,
    general.LabelError AS CalcError,  -- string, no cast to BIT
    TRY_CAST(shipping.ManualAdjust AS BIT) AS ManualAdjust,
    general.Status AS ErrorState,  
    general.ReferenceNum AS LabelId,
    TRY_CAST(shipping.PostalServiceId AS UNIQUEIDENTIFIER) AS fkPostageFileId,
    s.Height,
    s.Width,
    s.Depth
FROM 
    [linnworks].[staging]._airbyte_raw_processed_order_details d
CROSS APPLY 
    OPENJSON(d.ShippingInfo) WITH (
        ItemWeight FLOAT ''$.ItemWeight'',
        PackageTypeId UNIQUEIDENTIFIER ''$.PackageTypeId'',
        TotalWeight FLOAT ''$.TotalWeight'',
        ManualAdjust BIT ''$.ManualAdjust'',
        PostalServiceId UNIQUEIDENTIFIER ''$.PostalServiceId''
    ) AS shipping
CROSS APPLY
    OPENJSON(d.GeneralInfo) WITH (
        LabelError NVARCHAR(50) ''$.LabelError'',
        Status NVARCHAR(50) ''$.Status'',
        ReferenceNum NVARCHAR(100) ''$.ReferenceNum''
    ) AS general
CROSS APPLY
    OPENJSON(d.Items) WITH (
        StockItemId UNIQUEIDENTIFIER ''$.StockItemId''
    ) AS items
JOIN 
    [linnworks].[staging]._airbyte_raw_stock_items s
    ON items.StockItemId = s.StockItemId
WHERE 
    d.OrderId IS NOT NULL
    AND items.StockItemId IS NOT NULL;', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [StockItem_ExtendedProperties]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'StockItem_ExtendedProperties', 
		@step_id=14, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM lw.StockItem_ExtendedProperties;

INSERT INTO lw.StockItem_ExtendedProperties (
    pkRowId,
    fkStockItemId,
    PropertyName,
    PropertyValue,
    PropertyType
)
SELECT
    TRY_CAST(ep.pkRowId AS UNIQUEIDENTIFIER) AS pkRowId,
    TRY_CAST(d.StockItemId AS UNIQUEIDENTIFIER) AS fkStockItemId,
    ep.ProperyName AS PropertyName,  -- Alias the misspelled JSON key here
    ep.PropertyValue,
    ep.PropertyType
FROM 
    [linnworks].[staging].[_airbyte_raw_stock_items] d
CROSS APPLY 
    OPENJSON(d.ItemExtendedProperties) 
    WITH (
        pkRowId VARCHAR(255) ''$.pkRowId'',
        ProperyName VARCHAR(255) ''$.ProperyName'',  -- Original key from JSON
        PropertyValue VARCHAR(255) ''$.PropertyValue'',
        PropertyType VARCHAR(255) ''$.PropertyType''
    ) ep
WHERE d.ItemExtendedProperties IS NOT NULL;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [StockLocation]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'StockLocation', 
		@step_id=15, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO lw.stocklocation (
    pkStockLocationId,
    Location,
    Address1,
    Address2,
    City,
    County,
    Country,
    ZipCode,
    bLogicalDelete,
    IsNotTrackable,
    LocationTag,
    IsFulfillmentCenter,
    CountInOrderUntilAcknowledgement,
    FulfilmentCenterDeductStockWhenProcessed,
    IsWarehouseManaged
)
SELECT
    TRY_CAST(StockLocationId AS UNIQUEIDENTIFIER) AS pkStockLocationId,
    LocationName AS Location,
    Address1,
    Address2,
    City,
    County,
    Country,
    ZipCode,
    NULL AS bLogicalDelete,
    TRY_CAST(IsNotTrackable AS BIT) AS IsNotTrackable,
    LocationTag,
    TRY_CAST(IsFulfillmentCenter AS BIT) AS IsFulfillmentCenter,
    TRY_CAST(NULLIF(LTRIM(RTRIM(CountInOrderUntilAcknowledgement)), '''') AS INT) AS CountInOrderUntilAcknowledgement,
    TRY_CAST(FulfilmentCenterDeductStockWhenProcessed AS BIT) AS FulfilmentCenterDeductStockWhenProcessed,
    TRY_CAST(IsWarehouseManaged AS BIT) AS IsWarehouseManaged
FROM 
    [linnworks].[staging]._airbyte_raw_stock_locations AS data
WHERE 
    StockLocationId IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM lw.stocklocation s
        WHERE s.pkStockLocationId = TRY_CAST(data.StockLocationId AS UNIQUEIDENTIFIER)
    );
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [ListCountries]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ListCountries', 
		@step_id=16, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM lw.ListCountries;
INSERT INTO lw.ListCountries (
    pkCountryId,  
    cCountry,  
    cCurrency,  
    nPostageCostPerKg,  
    rowguid,  
    TaxRate  
)
SELECT
    TRY_CAST(JSON_VALUE(CAST(d.CustomerInfo AS NVARCHAR(MAX)), ''$.Address.CountryId'') AS UNIQUEIDENTIFIER) AS pkCountryId,
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
    AND d.Items IS NOT NULL;', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [StockLevel]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'StockLevel', 
		@step_id=17, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete FROM [linnworks].[lw].[StockLevel]
INSERT INTO lw.StockLevel (
    fkStockItemId,
    fkStockLocationId,
    Quantity,
    OnOrder,
    CurrentStockValue,
    MinimumLevel,
    AutoAdjust,
    LastUpdateDate,
    LastUpdateOperation,
    rowid,
    PendingUpdate,
    InOrderBook,
    JIT
)
SELECT
    j.StockItemId,
    j.StockLocationId,
    j.StockLevel,
    j.InOrders,
    j.StockValue,
    j.MinimumLevel,
    j.AutoAdjust,
    j.LastUpdateDate,
    j.LastUpdateOperation,
    j.rowid,
    j.PendingUpdate,
    j.InOrderBook,
    j.JIT
FROM [linnworks].[staging].[_airbyte_raw_stock_items] d
CROSS APPLY OPENJSON(d.StockLevels) 
WITH (
    StockItemId UNIQUEIDENTIFIER ''$.StockItemId'',
    StockLocationId UNIQUEIDENTIFIER ''$.Location.StockLocationId'',
    StockLevel INT ''$.StockLevel'',
    InOrders INT ''$.InOrders'',
    StockValue FLOAT ''$.StockValue'',
    MinimumLevel INT ''$.MinimumLevel'',
    AutoAdjust BIT ''$.AutoAdjust'',
    LastUpdateDate DATETIME ''$.LastUpdateDate'',
    LastUpdateOperation VARCHAR(64) ''$.LastUpdateOperation'',
    rowid UNIQUEIDENTIFIER ''$.rowid'',
    PendingUpdate BIT ''$.PendingUpdate'',
    InOrderBook INT ''$.InOrderBook'',
    JIT BIT ''$.JIT''
) AS j', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [FinalOrderItems]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'FinalOrderItems', 
		@step_id=18, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM [linnworks].[lw].[final_orderitems]
WHERE source = ''linnworks'';

INSERT INTO [linnworks].[lw].[final_orderitems] (
    Final_sku,
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
SELECT DISTINCT
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' 
            THEN OI.SubItemSKU
        ELSE OI.ParentSKU
    END AS final_sku,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' AND OI.SubItemQty <> 0
            THEN OI.SubItemQty
        ELSE OI.ParentQty
    END AS final_quantity,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' AND OI.SubItemSellPrice <> 0
            THEN OI.SubItemSellPrice
        ELSE OI.ParentSellPrice
    END AS final_price,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' AND OI.SubItemUnitCost <> 0
            THEN OI.SubItemUnitCost
        ELSE OI.ParentUnitCost
    END AS final_cost,
    
    (
        CASE 
            WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' 
                 AND OI.SubItemSellPrice <> 0 AND OI.SubItemQty <> 0
                THEN OI.SubItemSellPrice * OI.SubItemQty
            ELSE OI.ParentSellPrice * OI.ParentQty
        END
    ) AS TotalFinalPrice,
    
    (
        CASE 
            WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' 
                 AND OI.SubItemUnitCost <> 0 AND OI.SubItemQty <> 0
                THEN OI.SubItemUnitCost * OI.SubItemQty
            ELSE OI.ParentUnitCost * OI.ParentQty
        END
    ) AS TotalFinalCost,
    
    O.dProcessedOn AS final_date,
    
    CASE
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' 
            THEN OI.ParentSKU
        ELSE NULL
    END AS kitsku,
    
    ''linnworks'' AS source,
    OI.ParentTitle,
    OI.OrderId,
    OI.LocationId
FROM [linnworks].[staging].[processed_orders] O
JOIN [linnworks].[lw].[OrderItem_full] OI
    ON O.pkOrderId = OI.OrderId;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [OrderSales]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'OrderSales', 
		@step_id=19, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM [linnworks].[lw].[Order_sales]
WHERE source = ''linnworks'';

INSERT INTO [linnworks].[lw].[Order_sales] (
    OrderDate,
    itemID,
    UnitCost,
    TotalCost,
    UnitPrice,
    TotalPrice,
    UnitTax,
    TotalTax,
    TotalIncTax,
    Quantity,
    SubItemSKU,
    SubItemUnitCost,
    SubItemQty,
    fkOrderId,
    FkLocationId,
    ItemSource,
    fkStockItemId,
    source
)
SELECT DISTINCT
    TRY_CAST(O.dReceivedDate AS DATETIME) AS OrderDate,
    
    CASE 
        WHEN OI.SubItemSKU IS NOT NULL AND LTRIM(RTRIM(OI.SubItemSKU)) <> '''' 
        THEN OI.SubItemSKU
        ELSE OI.ParentSKU
    END AS itemID,
    
    OI.ParentUnitCost AS UnitCost,
    OI.ParentUnitCost * OI.ParentQty AS TotalCost,
    OI.ParentSellPrice AS UnitPrice,
    OI.ParentSellPrice * OI.ParentQty AS TotalPrice,
    OI.ParentTax / NULLIF(OI.ParentQty,0) AS UnitTax,
    OI.ParentTax AS TotalTax,
    OI.ParentTotalIncTax AS TotalIncTax,
    OI.ParentQty AS Quantity,
    
    OI.SubItemSKU AS SubItemSKU,
    OI.SubItemUnitCost AS SubItemUnitCost,
    OI.SubItemQty AS SubItemQty,
    
    CAST(OI.OrderId AS VARCHAR(36)) AS fkOrderId,
    
    OI.LocationId AS FkLocationId,
    OI.ParentItemSource AS ItemSource,
    
    CAST(OI.ParentStockItemId AS VARCHAR(36)) AS fkStockItemId,
    
    ''linnworks'' AS source
FROM 
    [linnworks].[staging].[processed_orders] O
JOIN 
    [linnworks].[lw].[OrderItem_full] OI
    ON O.pkOrderID = CAST(OI.OrderId AS VARCHAR(255));
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [purchasing]     ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'purchasing', 
		@step_id=20, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM [linnworks].[lw].[purchasing]
WHERE source = ''linnworks'';

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
    ''linnworks'' AS source
FROM [linnworks].[staging].[FullPurchaseOrders] fpo
JOIN [linnworks].[lw].[PurchaseOrders] po
    ON fpo.PurchaseOrderHeader_pkPurchaseID = po.pkPurchaseID
JOIN [linnworks].[lw].[Supplier] s
    ON po.fkSupplierID = s.pkSupplierId;
', 
		@database_name=N'linnworks', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'linnworks', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=6, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20250818, 
		@active_end_date=99991231, 
		@active_start_time=93000, 
		@active_end_time=235959, 
		@schedule_uid=N'8110707c-062c-40eb-aad3-8174fc7fb48a'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

