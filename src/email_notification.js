function checkJobFailures() {
 var server =  *****
 var database = *****
 var username = *****
 var password = *****
 var url = 'jdbc:sqlserver://' + server + ':1433;databaseName=' + database;
  // Connect to the SQL Server
 var conn = Jdbc.getConnection(url, username, password);
  // This is the main sql script that checks for the last 5 days
 var query = `
   SELECT
       j.name AS JobName,
       s.step_name AS StepName,
       CASE
           WHEN h.run_status = 1 THEN 'Success'
           WHEN h.run_status = 0 THEN 'Failure'
           ELSE 'Other'
       END AS JobStatus,
       CAST(CAST(h.run_date AS VARCHAR(8)) AS DATETIME) +
       (h.run_duration / 10000 * 3600 + ((h.run_duration / 100) % 100) * 60 + (h.run_duration % 100)) / 86400.0 AS LastRunDateTime,
       h.run_duration AS LastRunDuration,
       h.message AS LastRunMessage
   FROM msdb.dbo.sysjobs j
   JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
   JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id AND h.step_id = s.step_id
   WHERE j.name = 'airbyte_etl'
     AND h.run_status = 0  -- Only failures
     AND h.run_date >= CONVERT(VARCHAR(8), GETDATE() - 5, 112)  -- Last 5 days
   ORDER BY h.run_date DESC, h.run_duration DESC;
 `;
 
 var stmt = conn.createStatement();
 var results = stmt.executeQuery(query);
 var messageBody = '';
 while (results.next()) {
   var jobName = results.getString('JobName');
   var stepName = results.getString('StepName');
   var jobStatus = results.getString('JobStatus');
   var lastRunDateTime = results.getString('LastRunDateTime');
   var lastRunDuration = results.getString('LastRunDuration');
   var lastRunMessage = results.getString('LastRunMessage');
  

   if (jobStatus === 'Failure') {
     messageBody += 'Job Name: ' + jobName + '\n';
     messageBody += 'Step Name: ' + stepName + '\n';
     messageBody += 'Status: ' + jobStatus + '\n';
     messageBody += 'Last Run DateTime: ' + lastRunDateTime + '\n';
     messageBody += 'Duration: ' + lastRunDuration + '\n';
     messageBody += 'Message: ' + lastRunMessage + '\n\n';
   }
 }

 results.close();
 stmt.close();
 conn.close();

 if (messageBody) {
   var emailSubject = 'SQL Job Failures Detected';
   var recipient = '';
   var emailBody = 'The following ETL jobs have failed:\n\n' + messageBody;
   MailApp.sendEmail(recipient, emailSubject, emailBody);
 } else {
   Logger.log('No failures found.');
 }
}