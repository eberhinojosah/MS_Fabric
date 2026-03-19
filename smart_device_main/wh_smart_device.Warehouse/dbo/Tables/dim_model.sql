CREATE TABLE [dbo].[dim_model] (

	[model_id] int NULL, 
	[category_id] int NULL, 
	[model] varchar(8000) NULL, 
	[code_name] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);