CREATE DATABASE CarDataAnalysis;
USE DATABASE CarDataAnalysis;
CREATE SCHEMA ELTWorkflow;
USE SCHEMA ELTWorkflow;
CREATE OR REPLACE STAGE CarDataStage;
CREATE OR REPLACE TABLE RawData (
    Name VARCHAR,
    Location VARCHAR,
    Year INTEGER,
    Kilometers_Driven INTEGER,
    Fuel_Type VARCHAR,
    Transmission VARCHAR,
    Owner_Type VARCHAR,
    Mileage VARCHAR,
    Engine VARCHAR,
    Power VARCHAR,
    Seats INTEGER,
    New_Price VARCHAR,
    Price FLOAT
);
COPY INTO RawData
FROM @CarDataStage/dataog.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE);
CREATE OR REPLACE TABLE TransformedData AS
SELECT
    Name,
    Location,
    Year,
    Kilometers_Driven,
    Fuel_Type,
    Transmission,
    Owner_Type,
    TRY_CAST(REGEXP_SUBSTR(Mileage, '\\d+\\.?\\d*') AS FLOAT) AS Mileage,
    REGEXP_SUBSTR(Engine, '\\d+') AS Engine,
    REGEXP_SUBSTR(Power, '\\d+\\.?\\d*') AS Power,
    Seats,
    New_Price,
    Price
FROM RawData
WHERE Mileage IS NOT NULL AND Engine IS NOT NULL AND Power IS NOT NULL;
SELECT * FROM TRANSFORMEDDATA;
SELECT COUNT(*) AS RowCount
FROM TRANSFORMEDDATA;
CREATE OR REPLACE TABLE Brands (
    BrandID INTEGER AUTOINCREMENT PRIMARY KEY,
    BrandName VARCHAR UNIQUE
);
INSERT INTO Brands (BrandName)
SELECT DISTINCT
    CASE
        WHEN Name LIKE 'Land Rover%' THEN 'Land Rover'
        WHEN Name LIKE 'Mercedes-Benz%' THEN 'Mercedes-Benz'
        WHEN Name LIKE 'BMW%' THEN 'BMW'
        ELSE SPLIT_PART(Name, ' ', 1) -- Default: First word
    END AS Brand
FROM TRANSFORMEDDATA;
select * from Brands;
CREATE OR REPLACE TABLE Models (
    ModelID INTEGER AUTOINCREMENT PRIMARY KEY,
    BrandID INTEGER REFERENCES Brands(BrandID),
    ModelNameAndVariant VARCHAR,
    Engine_CC INTEGER,
    Power_BHP FLOAT,
    Mileage_Value FLOAT,
    Seats INTEGER
);
INSERT INTO Models (BrandID, ModelNameAndVariant, Engine_CC, Power_BHP, Mileage_Value, Seats)
SELECT DISTINCT
    b.BrandID,
    -- Extract Model and Variant
    CASE
        -- Special handling for "Land Rover"
        WHEN t.Name LIKE 'Land Rover%' THEN
            TRIM(SUBSTR(t.Name, LENGTH('Land Rover') + 2))
        -- Special handling for "Mercedes-Benz"
        WHEN t.Name LIKE 'Mercedes-Benz%' THEN
            TRIM(SUBSTR(t.Name, LENGTH('Mercedes-Benz') + 2))
        -- Special handling for "BMW"
        WHEN t.Name LIKE 'BMW%' THEN
            TRIM(SUBSTR(t.Name, LENGTH('BMW') + 2))
        -- Default for other brands: Extract everything after the first word (brand name)
        ELSE
            TRIM(SUBSTR(t.Name, LENGTH(SPLIT_PART(t.Name, ' ', 1)) + 2))
    END AS ModelNameAndVariant,
    Engine,
    Power,
    Mileage,
    Seats
FROM TRANSFORMEDDATA t
JOIN Brands b ON
    CASE
        -- Match multi-word brands
        WHEN t.Name LIKE 'Land Rover%' THEN 'Land Rover'
        WHEN t.Name LIKE 'Mercedes-Benz%' THEN 'Mercedes-Benz'
        WHEN t.Name LIKE 'BMW%' THEN 'BMW'
        -- Default for single-word brands
        ELSE SPLIT_PART(t.Name, ' ', 1)
    END = b.BrandName;
--To handle duplicate entries in Models table
CREATE TEMPORARY TABLE DeduplicatedModels AS
SELECT
    ModelID,
    BrandID,
    ModelNameAndVariant,
    Engine_CC,
    Power_BHP,
    Mileage_Value,
    Seats
FROM (
    SELECT
        ModelID,
        BrandID,
        ModelNameAndVariant,
        Engine_CC,
        Power_BHP,
        Mileage_Value,
        Seats,
        ROW_NUMBER() OVER (
            PARTITION BY ModelNameAndVariant
            ORDER BY ModelID ASC
        ) AS row_num
    FROM Models
) AS RankedModels
WHERE row_num = 1;
TRUNCATE TABLE Models;
INSERT INTO Models
SELECT * FROM DeduplicatedModels;
drop table DeduplicatedModels;
select * from models order by ModelNameAndVariant;
CREATE OR REPLACE TABLE Locations (
    LocationID INTEGER AUTOINCREMENT PRIMARY KEY,
    LocationName VARCHAR UNIQUE
);
INSERT INTO Locations (LocationName)
SELECT DISTINCT Location FROM TRANSFORMEDDATA;
CREATE OR REPLACE TABLE Cars (
    CarID INTEGER AUTOINCREMENT PRIMARY KEY,
    ModelID INTEGER REFERENCES Models(ModelID),
    LocationID INTEGER REFERENCES Locations(LocationID),
    Year INTEGER,
    Kilometers_Driven INTEGER,
    Transmission VARCHAR,
    Fuel_Type VARCHAR,
    Owner_Type VARCHAR,
    Price FLOAT
);
CREATE TEMPORARY TABLE TempBrandExtraction AS
SELECT
    r.Name,
    CASE
        WHEN r.Name LIKE 'Land Rover%' THEN 'Land Rover'
        WHEN r.Name LIKE 'Mercedes-Benz%' THEN 'Mercedes-Benz'
        WHEN r.Name LIKE 'BMW%' THEN 'BMW'
        ELSE REGEXP_SUBSTR(r.Name, '^[^ ]+')
    END AS ExtractedBrand,
      CASE
        -- Special handling for "Land Rover"
        WHEN r.Name LIKE 'Land Rover%' THEN
            TRIM(SUBSTR(r.Name, LENGTH('Land Rover') + 2))
        -- Special handling for "Mercedes-Benz"
        WHEN r.Name LIKE 'Mercedes-Benz%' THEN
            TRIM(SUBSTR(r.Name, LENGTH('Mercedes-Benz') + 2))
        -- Special handling for "BMW"
        WHEN r.Name LIKE 'BMW%' THEN
            TRIM(SUBSTR(r.Name, LENGTH('BMW') + 2))
        -- Default for other brands: Extract everything after the first word (brand name)
        ELSE
            TRIM(SUBSTR(r.Name, LENGTH(SPLIT_PART(r.Name, ' ', 1)) + 2))
    END AS ModelNameAndVariant,
    r.Year,
    r.Kilometers_Driven,
    r.Transmission,
    r.Fuel_Type,
    r.Owner_Type,
    TRY_CAST(r.Price AS FLOAT) AS Price,
    r.Location,
    r.Power
FROM transformeddata r;
INSERT INTO Cars (ModelID, LocationID, Year, Kilometers_Driven, Transmission, Fuel_Type, Owner_Type, Price)
SELECT
    ModelID,
    LocationID,
    Year,
    Kilometers_Driven,
    Transmission,
    Fuel_Type,
    Owner_Type,
    Price
FROM (
    SELECT
        m.ModelID,
        l.LocationID,
        r.Year,
        r.Kilometers_Driven,
        r.Transmission,
        r.Fuel_Type,
        r.Owner_Type,
        r.Price,
        ROW_NUMBER() OVER (
            PARTITION BY b.BrandName, r.ModelNameAndVariant, r.year, r.Kilometers_Driven, r.Transmission, r.Fuel_Type, r.Owner_Type
            ORDER BY r.Kilometers_Driven DESC
        ) AS row_num
    FROM TempBrandExtraction r
    JOIN Brands b ON r.ExtractedBrand = b.BrandName
    JOIN Models m ON m.ModelNameAndVariant = r.ModelNameAndVariant
    JOIN Locations l ON r.Location = l.LocationName
) AS RankedCars
WHERE row_num = 1;
-- Drop the temporary table
DROP TABLE TempBrandExtraction;
SELECT ModelNameAndVariant, COUNT(*)
FROM Models
GROUP BY ModelNameAndVariant
HAVING COUNT(*) > 1;
SELECT BrandName, COUNT(*)
FROM Brands
GROUP BY BrandName
HAVING COUNT(*) > 1;
SELECT LocationName, COUNT(*)
FROM Locations
GROUP BY LocationName
HAVING COUNT(*) > 1;
select * from cars;
-- Average Price by brand
SELECT
    b.BrandName,
    AVG(c.Price) AS Average_Price
FROM Cars c
JOIN Models m ON c.ModelID = m.ModelID
JOIN Brands b ON m.BrandID = b.BrandID
GROUP BY b.BrandName
ORDER BY Average_Price DESC;
--Most Common Fuel Types by Location
SELECT
    l.LocationName,
    c.Fuel_Type,
    COUNT(*) AS Count
FROM Cars c
JOIN Locations l ON c.LocationID = l.LocationID
GROUP BY l.LocationName, c.Fuel_Type
ORDER BY l.LocationName, Count DESC;
--Average Mileage and Power for Specific Models&Variants
SELECT
    m.MODELNAMEANDVARIANT,
    AVG(m.Mileage_Value) AS Average_Mileage,
    AVG(m.Power_BHP) AS Average_Power
FROM Models m
JOIN Cars c ON c.ModelID = m.ModelID
GROUP BY m.MODELNAMEANDVARIANT
ORDER BY Average_Mileage DESC;
--Count of Cars by Transmission Type
SELECT
    c.Transmission,
    COUNT(*) AS Car_Count
FROM Cars c
GROUP BY c.Transmission
ORDER BY Car_Count DESC;
--Average Kilometers Driven by Car Model
SELECT
    m.modelnameandvariant,
    AVG(c.Kilometers_Driven) AS Average_Kilometers
FROM Cars c
JOIN Models m ON c.ModelID = m.ModelID
GROUP BY m.modelnameandvariant
ORDER BY Average_Kilometers DESC;
--Most Popular Car Models in Each Location
WITH RankedModels AS (
    SELECT
        l.LocationName,
        m.modelnameandvariant,
        COUNT(*) AS Car_Count,
        ROW_NUMBER() OVER (PARTITION BY l.LocationName ORDER BY COUNT(*) DESC) AS Rank
    FROM Cars c
    JOIN Locations l ON c.LocationID = l.LocationID
    JOIN Models m ON c.ModelID = m.ModelID
    GROUP BY l.LocationName, m.modelnameandvariant
)
SELECT
    LocationName,
    modelnameandvariant,
    Car_Count
FROM RankedModels
WHERE Rank <= 5
ORDER BY LocationName, Car_Count DESC;
--Price Distribution by Fuel Type
SELECT
    c.Fuel_Type,
    COUNT(*) AS Count,
    AVG(c.Price) AS Average_Price,
    MIN(c.Price) AS Min_Price,
    MAX(c.Price) AS Max_Price
FROM Cars c
GROUP BY c.Fuel_Type
ORDER BY Fuel_Type;