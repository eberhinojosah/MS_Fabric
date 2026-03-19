CREATE TABLE [dbo].[dim_operating_system] (

	[os_id] int NULL, 
	[platform] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);