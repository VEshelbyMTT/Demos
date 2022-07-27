USE dp203_demo


-- I want to find out how many rows I have 

SELECT COUNT(*)
FROM openrowset(
    BULK 'latest/ecdc_cases.parquet',
                DATA_SOURCE = 'ecdc_cases',
                FORMAT ='parquet') AS ecdc_cases_nov_13

WHERE ecdc_cases_nov_13.date_rep = '2020-11-13'

-- Answer : 203 

-- I want to find out which country has the highest deaths/cases

SELECT deaths, countries_and_territories
FROM openrowset(
    BULK 'latest/ecdc_cases.parquet',
                DATA_SOURCE = 'ecdc_cases',
                FORMAT ='parquet') AS cases

WHERE cases.date_rep = '2020-11-13'
ORDER BY cases.deaths DESC;
GO;
-- Answer: Brazil 