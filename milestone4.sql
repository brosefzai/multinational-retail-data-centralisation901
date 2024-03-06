#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 28 11:33:10 2024

@author: frasier
"""

--MILESTONE4

--task 1:
SELECT country_code AS country, COUNT(country_code) AS total_no_stores
FROM dim_store_details 
GROUP BY country_code
ORDER BY total_no_stores desc;

--task 2:
SELECT locality, COUNT(locality) AS total_no_stores
FROM dim_store_details 
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

--task 3:
SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price::numeric),2) AS total_sales, "month"
FROM orders_table
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY "month"
ORDER BY total_sales DESC
LIMIT 6;

--task 4:
SELECT COUNT(DISTINCT(date_uuid)) as numbers_of_sales, SUM(product_quantity) as product_quantity_count, 
CASE WHEN store_type IN ('Local', 'Mall Kiosk', 'Outlet', 'Super Store') THEN 'Offline' WHEN store_type = 'Web Portal' THEN 'Web' END location
FROM orders_table
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY product_quantity_count ASC;

--task 5:
WITH total_sales_per_store_type AS (
    SELECT store_type,
           ROUND(SUM(orders_table.product_quantity * dim_products.product_price::numeric), 2) AS total_sales
    FROM dim_store_details
    INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
    INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY store_type
)
SELECT store_type,
       total_sales,
       ROUND((total_sales / SUM(total_sales) OVER ()) * 100, 2) AS "percentage_total(%)"
FROM total_sales_per_store_type
ORDER BY total_sales DESC;

--task 6:
SELECT ROUND(SUM(product_quantity * product_price::numeric), 2) as total_sales, "year", "month" FROM orders_table
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY "year", "month"
ORDER BY total_sales DESC
LIMIT 10;

--task 7:
SELECT SUM(staff_numbers) as total_staff_numbers, country_code FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--task 8:
SELECT ROUND(SUM(product_quantity * product_price::numeric), 2) as total_sales, store_type, country_code FROM orders_table
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;

--task 9:
WITH concat_time AS(
SELECT "year", CONCAT(year, '-', month, '-', day, ' ', "timestamp")::TIMESTAMP (1) WITHOUT TIME ZONE as c_time from dim_date_times
),
offset_time AS(
SELECT "year", c_time, (LEAD(c_time, 1) OVER(ORDER BY c_time)) as offset_time FROM concat_time
),
actual_time AS(
SELECT "year", (AVG(offset_time - c_time))::text as att FROM offset_time
GROUP BY "year"	
)

SELECT "year", CONCAT('"hours": ', substr(att, 2, 1), ', "minutes": ', substr(att, 4, 2), ', "seconds": ', substr(att, 7, 2), ', "milliseconds": ', substr(att, 10, 2)) as actual_time_taken FROM actual_time
ORDER BY att DESC
LIMIT 5;



