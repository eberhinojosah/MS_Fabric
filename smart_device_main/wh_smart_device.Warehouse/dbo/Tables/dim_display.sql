CREATE TABLE [dbo].[dim_display] (

	[display_id] int NULL, 
	[resolution] varchar(8000) NULL, 
	[color_depth] varchar(8000) NULL, 
	[illumination] varchar(8000) NULL, 
	[refresh_rate] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);