CREATE TABLE [dbo].[dim_time] (

	[date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[day] int NULL, 
	[month_name] varchar(8000) NULL, 
	[day_name] varchar(8000) NULL, 
	[quarter] varchar(8000) NULL
);