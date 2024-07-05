# Python-ETL
A Python ETL for uploading .csv files in bulk to a MS SQL Server database table.


Herein is an ETL python tool for uploading *.csv* files in bulk to a MS SQL Server database table.  The upload files are initially stored in an UPLOAD folder, then are concatenated and all records are uploaded to a temporary database table, dbo.temp_Persons.  A stored procedure, dbo.sp_persons_upload, is executed to insert records from the temporary table into a final table, dbo.Persons, while adding an AUTO_ID primary key and creation date. The original .csv upload files are copied to an ARCHIVE folder then deleted from the UPLOAD folder. A log file is created to document the files, time of upload, success of operation, and any errors. Note, if the database requires a username and password, those will need to be incorporated into the connection_string() function starting on line 82 as: USERNAME= {username}; PASSWORD= {password};.

Database tables and stored procedure (sproc) are described below:

dbo.temp_Persons (table)
FIRST_NAME (varchar(255), not null)
LAST_NAME (varchar(255), not null)
AGE (int, null)

dbo.Persons (table)
AUTO_ID (PK, int, not null)
FIRST_NAME (varchar(255), not null)
LAST_NAME (varchar(255), not null)
AGE (int, null)
CREATE_DATE (datetime, null)

dbo.sp_persons_upload (sproc)
SET NOCOUNT ON;
INSERT INTO [Persons]([FIRST_NAME],[LAST_NAME],[AGE])
SELECT [FIRST_NAME],[LAST_NAME],[AGE]
FROM [SQL_TESTING].[dbo].[temp_Persons];
DELETE FROM [SQL_TESTING].[dbo].[temp_Persons];
GO

This script can be modified for use with other types of databases.  Furthermore, the ETL can be automated by creating a Windows Executable *.bat file to run Python and taking advantage of Windows Task Scheduler.


