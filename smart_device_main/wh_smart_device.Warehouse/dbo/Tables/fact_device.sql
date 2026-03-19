CREATE TABLE [dbo].[fact_device] (

	[device_id] int NULL, 
	[brand_id] int NULL, 
	[model_id] int NULL, 
	[display_id] int NULL, 
	[camera_id] int NULL, 
	[connectivity_id] int NULL, 
	[os_id] int NULL, 
	[physical_spec_id] int NULL, 
	[released_announced] date NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);