-- Explore a public ecdc covid dataset within Synapse Serverless SQL 

USE master;
GO;

-- creating a database that provides optimal performance for parquet/cosmos (collate)
CREATE DATABASE dp203_demo
      COLLATE Latin1_General_100_BIN2_UTF8;
GO; 

-- Switch to our shiny new database
USE dp203_demo;

-- Now we have created the database we want to use, let us populate it with an opensource dataset
-- creating an open connection string to find and populate our data!

CREATE EXTERNAL DATA SOURCE ecdc_cases WITH (
    LOCATION = 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/'
)
    ; 
GO;

-- **GOAL: What do I want to find out**: 
-- For November 13 2020, I want to: 
--- I want to find out how many rows I have 
--- I want to find out which country has the highest deaths/cases

SELECT *
FROM OPENROWSET(
    BULK 'latest/ecdc_cases.parquet',
                DATA_SOURCE = 'ecdc_cases',
                FORMAT ='parquet') AS cases





     