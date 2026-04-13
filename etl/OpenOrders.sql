DELETE FROM [linnworks].[staging].[processed_orders]
WHERE status = 'open';

INSERT INTO [linnworks].[staging].[processed_orders] (
    pkOrderID,
    dProcessedOn,
    dReceivedDate,
    status
)
SELECT  
    OrderId AS pkOrderID,                                         
    NULL AS dProcessedOn,                                        
    JSON_VALUE(CAST(GeneralInfo AS NVARCHAR(MAX)), '$.ReceivedDate') AS dReceivedDate,  
    'open' AS status
FROM [linnworks].[staging].[_airbyte_raw_processed_order_details]
WHERE Processed = 0;
