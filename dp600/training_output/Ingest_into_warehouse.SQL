-- NOTE CAN ONLY BE DONE IN SAME WS

-- Create a table
CREATE TABLE [dbo].[dinosaurs_from_onelake] (
    DinoID VARCHAR(10),  -- Unique ID for each dinosau
    Species VARCHAR(100),   -- Species name
    Colour VARCHAR(50),     -- Colour of the dinosaur
    CountInDatacenters INT   -- Count in datacenters
);

-- Insert a table
INSERT INTO [dbo].[dinosaurs_from_onelake] 
SELECT * FROM [fabric_unified_lakehouse].[dbo].[dinosaur_ingest_to_warehouse];


/* 
CREATE TABLE [your_warehouse].[dbo].[new_table]
AS
SELECT 
FROM LHOrWH_table_1  name_1
INNER JOIN LHOrWH_table_2  name_2
ON name_1.key = name_2.key;
*/

/*
-- Insert


INSERT INTO [your_warehouse].[dbo].[new_table]
SELECT 
    name_1.key 
FROM [LHOrWH].[dbo].[table_1] name_1
INNER JOIN [LHOrWH].[dbo].[table_2] name_2
ON name_1.key = name_2.key;

--^^ limitations can lead to poor performance if doing singleton inserts
-- Best practice: CTAS > INSERT INTO - Use CTAS to create new_table -> DROP old_table -> ALTER new_table RENAME TO old_table

*/

/*
-- FROM EXTERNAL MSFT STORAGE 
COPY INTO test_parquet
FROM
'https://myaccount.blob.core.windows.net/myblobcontainer/folder1/*.parquet'
WITH (CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='<Your_SAS_Token>'))


-- Best practice: ingest files > 4mb | large csv bois? Partition them
*/


-- Remember for updates: Always add update statistics in your Store procedure scripts. 

--Best practices: https://learn.microsoft.com/en-us/fabric/data-warehouse/ingest-data#best-practices
