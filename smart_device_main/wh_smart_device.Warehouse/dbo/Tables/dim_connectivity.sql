CREATE TABLE [dbo].[dim_connectivity] (

	[connectivity_id] int NULL, 
	[usb] varchar(8000) NULL, 
	[usb_connector] varchar(8000) NULL, 
	[bluetooth] varchar(8000) NULL, 
	[nfc] varchar(8000) NULL, 
	[sim_card_slot] varchar(8000) NULL, 
	[ingestion_date] datetime2(6) NULL, 
	[file_date] varchar(8000) NULL
);