CREATE TABLE [dbo].[dim_camera] (

	[camera_id] int NULL, 
	[camera_resolution] varchar(8000) NULL, 
	[effective_pixel] varchar(8000) NULL, 
	[image_format] varchar(8000) NULL, 
	[video_recording] varchar(8000) NULL, 
	[flash] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);