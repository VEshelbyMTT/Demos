USE dp203_demo;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'DP203_!Data123';
GO;

--CREATE DATABASE SCOPED CREDENTIAL [BlobAccess]
--WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
--SECRET = '?sv=2020-08-04&ss=bfqt&srt=sc&sp=rwdlacupx&se=2024-08-01T04:29:08Z&st=2022-05-13T20:29:08Z&spr=https&sig=U8xQrhHjmxPwqcmV9sqxMXjgCL4Y48HI%2FOVljrTTymM%3D';
CREATE DATABASE SCOPED CREDENTIAL [WorkspaceIdentity]
WITH IDENTITY = 'Managed Identity';
GO;

-- Create connection string to where I intend to collect the data from 
CREATE EXTERNAL DATA SOURCE [ecdc_adls] WITH (
    CREDENTIAL =[WorkspaceIdentity],
    LOCATION = 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/'
); 
GO;

-- good practice: organize your tables and views in databases schemas
GO;
CREATE SCHEMA [ecdc_schema];
GO;

-- File format
CREATE EXTERNAL FILE FORMAT [ParquetFormat] WITH (  FORMAT_TYPE = PARQUET );
GO;
-- CREATE EXTERNAL FILE FORMAT [CsvFormat] WITH (  FORMAT_TYPE = DELIMITEDTEXT );
-- GO;

-- Creating an external table
CREATE EXTERNAL TABLE [ecdc_schema].[cases](
    date_rep                   DATE,
    day                        SMALLINT,
    month                      SMALLINT,
    year                       SMALLINT,
    cases                      SMALLINT,
    deaths                     SMALLINT,
    countries_and_territories  VARCHAR(256),
    geo_id                     VARCHAR(60),
    country_territory_code     VARCHAR(16),
    pop_data_2018              INT,
    continent_exp              VARCHAR(32),
    load_date                  DATETIME,
    iso_country                VARCHAR(16)
) WITH (
    LOCATION = 'latest/ecdc_cases.parquet',
    DATA_SOURCE= [ecdc_adls],
    FILE_FORMAT = [ParquetFormat]
);
GO;

/*
Where's my data? 
-> Metadata is stored not the actual database!
-> You can explore this in your data hub -> dp203_demo -> External Tables
SELECT TOP 1 *
FROM [ecdc_schema].[cases]

NOTE: use the smallest possible types for string and number columns to 
optimize performance of your queries where possible
*/
