CREATE DATABASE ZOMATO;

USE ZOMATO;

SELECT * FROM zomato_dataset


-- 1. A rolling or moving count of restaurants in Indian cities

WITH CT_Restaurant_Count AS (
    SELECT 
        City,
        Locality,
        COUNT(*) AS TOTAL_REST
    FROM zomato_dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
)
SELECT 
    City,
    Locality,
    TOTAL_REST,
    ROW_NUMBER() OVER (PARTITION BY City ORDER BY TOTAL_REST DESC) AS Locality_Rank,
    SUM(TOTAL_REST) OVER (PARTITION BY City ORDER BY TOTAL_REST DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Rolling_Total_Restaurants
FROM CT_Restaurant_Count
ORDER BY City, Locality_Rank;



-- Create Lookup Table For Country Name

CREATE TABLE CountryLookup (
    CountryCode INT PRIMARY KEY,
    CountryName VARCHAR(100)
);

INSERT INTO CountryLookup (CountryCode, CountryName) VALUES
(1, 'India'),
(14, 'Australia'),
(30, 'Brazil'),
(37, 'Canada'),
(94, 'Indonesia'),
(148, 'New Zealand'),
(162, 'Philippines'),
(166, 'Qatar'),
(184, 'Singapore'),
(189, 'South Africa'),
(191, 'Sri Lanka'),
(208, 'Turkey'),
(214, 'UAE'),
(215, 'United Kingdom'),
(216, 'United States');



-- 2. Percentage of Restaurants in All Countries

WITH TOTAL_COUNT AS (
    SELECT DISTINCT CountryCode, COUNT(RestaurantID) OVER () AS TOTAL_REST
    FROM Zomato_Dataset
),
CT1 AS (
    SELECT CountryCode, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    GROUP BY CountryCode
)
SELECT cl.CountryName, A.REST_COUNT, 
       ROUND((A.REST_COUNT / B.TOTAL_REST) * 100, 2) AS PERCENTAGE_REST
FROM CT1 A
JOIN TOTAL_COUNT B ON A.CountryCode = B.CountryCode
JOIN CountryLookup cl ON A.CountryCode = cl.CountryCode
ORDER BY PERCENTAGE_REST DESC;



-- 3 Percentage of Restaurants with Online Delivery per Country

WITH COUNTRY_REST AS (
    SELECT CountryCode, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    GROUP BY CountryCode
)
SELECT cl.CountryName, COUNT(A.RestaurantID) AS TOTAL_REST, 
       ROUND((COUNT(A.RestaurantID) / B.REST_COUNT) * 100, 2) AS PERCENTAGE_WITH_ONLINE_DELIVERY
FROM Zomato_Dataset A
JOIN COUNTRY_REST B ON A.CountryCode = B.CountryCode
JOIN CountryLookup cl ON A.CountryCode = cl.CountryCode
WHERE A.Has_Online_delivery = 'Yes'
GROUP BY A.CountryCode, B.REST_COUNT, cl.CountryName
ORDER BY TOTAL_REST DESC;


-- 4 City & Locality in India with Max Restaurants

WITH CT1 AS (
    SELECT City, Locality, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
)
SELECT Locality, REST_COUNT
FROM CT1
WHERE REST_COUNT = (SELECT MAX(REST_COUNT) FROM CT1);


-- 5.Types of Food in India in Locality with Max Restaurants

WITH CT1 AS (
    SELECT City, Locality, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
),
CT2 AS (
    SELECT Locality, REST_COUNT
    FROM CT1
    WHERE REST_COUNT = (SELECT MAX(REST_COUNT) FROM CT1)
)
SELECT A.Locality, B.Cuisines
FROM CT2 A
JOIN Zomato_Dataset B ON A.Locality = B.Locality;


-- 6.Most Popular Food in India where Max Restaurants Listed

WITH CT1 AS (
    SELECT City, Locality, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
),
CT2 AS (
    SELECT Locality, REST_COUNT
    FROM CT1
    WHERE REST_COUNT = (SELECT MAX(REST_COUNT) FROM CT1)
),
VF AS (
    SELECT CountryCode, City, Locality, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Cuisines, ',', numbers.n), ',', -1)) AS Cuisine
    FROM Zomato_Dataset
    JOIN (
        SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL
        SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) numbers
    ON CHAR_LENGTH(Cuisines) - CHAR_LENGTH(REPLACE(Cuisines, ',', '')) >= numbers.n - 1
    WHERE CountryCode = 1
)
SELECT A.Cuisine, COUNT(*) AS TOTAL
FROM VF A
JOIN CT2 B ON A.Locality = B.Locality
GROUP BY A.Cuisine
ORDER BY TOTAL DESC;


 
-- 7.Localities in India with Lowest Restaurants Listed

WITH CT1 AS (
    SELECT City, Locality, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
)
SELECT *
FROM CT1
WHERE REST_COUNT = (SELECT MIN(REST_COUNT) FROM CT1)
ORDER BY City;


-- 8.Restaurants Offering Table Booking in Locality with Max Restaurants in India

WITH CT1 AS (
    SELECT City, Locality, COUNT(RestaurantID) AS REST_COUNT
    FROM Zomato_Dataset
    WHERE CountryCode = 1
    GROUP BY City, Locality
),
CT2 AS (
    SELECT Locality, REST_COUNT
    FROM CT1
    WHERE REST_COUNT = (SELECT MAX(REST_COUNT) FROM CT1)
)
SELECT A.Locality, COUNT(*) AS TABLE_BOOKING_OPTION
FROM Zomato_Dataset A
JOIN CT2 B ON A.Locality = B.Locality
WHERE A.Has_Table_booking = 'Yes'
GROUP BY A.Locality;


-- 9. Effect of Rating with/without Table Booking in Connaught Place

SELECT 'WITH_TABLE' AS TABLE_BOOKING_OPT, COUNT(*) AS TOTAL_REST, ROUND(AVG(Rating), 2) AS AVG_RATING
FROM Zomato_Dataset
WHERE Has_Table_booking = 'Yes' AND Locality = 'Connaught Place'

UNION ALL

SELECT 'WITHOUT_TABLE' AS TABLE_BOOKING_OPT, COUNT(*) AS TOTAL_REST, ROUND(AVG(Rating), 2) AS AVG_RATING
FROM Zomato_Dataset
WHERE Has_Table_booking = 'No' AND Locality = 'Connaught Place';


-- 10.Average Rating of Restaurants Location-wise

SELECT cl.CountryName, City, Locality, COUNT(RestaurantID) AS TOTAL_REST, 
       ROUND(AVG(CAST(Rating AS DECIMAL(10,2))), 2) AS AVG_RATING
FROM Zomato_Dataset Z
JOIN CountryLookup cl ON Z.CountryCode = cl.CountryCode
GROUP BY cl.CountryName, City, Locality
ORDER BY TOTAL_REST DESC;

-- 11. Best Restaurants in India with Moderate Cost and Indian Cuisines

SELECT *
FROM Zomato_Dataset
WHERE CountryCode = 1
AND Has_Table_booking = 'Yes'
AND Has_Online_delivery = 'Yes'
AND Price_range <= 3
AND Votes > 1000
AND Average_Cost_for_two < 1000
AND Cuisines LIKE '%Indian%'
AND Rating > 4;      



-- 12.  Restaurants Offering Table Booking with High Rating by Price Range

SELECT Price_range, COUNT(*) AS NO_OF_REST
FROM Zomato_Dataset
WHERE Rating >= 4.5
AND Has_Table_booking = 'Yes'
GROUP BY Price_range; 


-- 13.  Top 5 Most Popular Cities by Number of Restaurants (India)

SELECT City, COUNT(RestaurantID) AS TOTAL_REST
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY City
ORDER BY TOTAL_REST DESC
LIMIT 5;

-- 14. Which Price Range has the Most Restaurants in India?

SELECT Price_range, COUNT(*) AS NO_OF_REST
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY Price_range
ORDER BY NO_OF_REST DESC;


-- 15. How Many Restaurants in India Offer Both Online Delivery and Table Booking?

SELECT COUNT(*) AS BOTH_DELIVERY_TABLE_BOOKING
FROM Zomato_Dataset
WHERE CountryCode = 1
AND Has_Table_booking = 'Yes'
AND Has_Online_delivery = 'Yes';


-- 16.  Average Cost for Two Across Cities in India

SELECT City, ROUND(AVG(Average_Cost_for_two), 2) AS AVG_COST_FOR_TWO
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY City
ORDER BY AVG_COST_FOR_TWO DESC;


-- 17. Top 5 Localities in India by Average Rating (with at least 10 Restaurants)

SELECT Locality, COUNT(*) AS TOTAL_REST, ROUND(AVG(Rating), 2) AS AVG_RATING
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY Locality
HAVING TOTAL_REST >= 10
ORDER BY AVG_RATING DESC
LIMIT 5;


-- 18. Distribution of Restaurants by Rating Bands (India)

SELECT 
    CASE 
        WHEN Rating < 2 THEN 'Below 2'
        WHEN Rating >= 2 AND Rating < 3 THEN '2-3'
        WHEN Rating >= 3 AND Rating < 4 THEN '3-4'
        WHEN Rating >= 4 AND Rating < 4.5 THEN '4-4.5'
        WHEN Rating >= 4.5 THEN '4.5+'
    END AS Rating_Band,
    COUNT(*) AS TOTAL_REST
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY Rating_Band
ORDER BY Rating_Band;


-- 19. Average Rating by Price Range in India

SELECT Price_range, ROUND(AVG(Rating), 2) AS AVG_RATING
FROM Zomato_Dataset
WHERE CountryCode = 1
GROUP BY Price_range
ORDER BY Price_range;


-- 20. Which Cuisines are Offered by Most Restaurants in India?


-- Again splitting Cuisines — simplified using SUBSTRING_INDEX trick:  

SELECT Cuisine, COUNT(*) AS TOTAL
FROM (
    SELECT TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Cuisines, ',', numbers.n), ',', -1)) AS Cuisine
    FROM Zomato_Dataset
    JOIN (
        SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL
        SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) numbers
    ON CHAR_LENGTH(Cuisines) - CHAR_LENGTH(REPLACE(Cuisines, ',', '')) >= numbers.n - 1
    WHERE CountryCode = 1
) AS CuisineList
GROUP BY Cuisine
ORDER BY TOTAL DESC
LIMIT 10;


-- 21. Find Restaurants with Rating ≥ 4.5 and Moderate Price Range in India (Top 10)

SELECT RestaurantName, City, Locality, Rating, Price_range, Average_Cost_for_two
FROM Zomato_Dataset
WHERE CountryCode = 1
AND Rating >= 4.5
AND Price_range <= 3
ORDER BY Rating DESC, Votes DESC
LIMIT 10; 

