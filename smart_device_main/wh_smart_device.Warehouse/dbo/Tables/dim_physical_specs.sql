CREATE TABLE [dbo].[dim_physical_specs] (

	[physical_spec_id] int NULL, 
	[width] varchar(8000) NULL, 
	[height] varchar(8000) NULL, 
	[depth] varchar(8000) NULL, 
	[mass] varchar(8000) NULL, 
	[ram_capacity] varchar(8000) NULL, 
	[controller] varchar(8000) NULL, 
	[NominaBatteryCapacity] varchar(8000) NULL, 
	[EstimateBatteryLife] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);