CREATE TABLE [dbo].[dim_brand] (

	[brand_id] int NULL, 
	[brand] varchar(8000) NULL, 
	[hardware_designer] varchar(8000) NULL, 
	[manufacturer] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);