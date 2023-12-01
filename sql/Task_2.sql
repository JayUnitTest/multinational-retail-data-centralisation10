/*Task 2: Which locations currently have the most stores?*/

/*Removing the 12 null data */

SELECT store_code
FROM dim_store_details
WHERE locality IS NULL;

DELETE FROM orders_table
WHERE store_code IN (
  'LA-55069B69', 'FA-6A7ABBAD', 'SE-8DF6557B', 'BU-B5B6F452',
  'BU-3FC2A064', 'ST-119B11EA', 'SI-ECD52CD9', 'GR-250FBD0C',
  'AB-FCFAB1A9', 'RU-9F1136B4', 'ME-9940FF73', 'CL-DA786EF6'
);

DELETE FROM dim_store_details
WHERE store_code IN (
  'LA-55069B69', 'FA-6A7ABBAD', 'SE-8DF6557B', 'BU-B5B6F452',
  'BU-3FC2A064', 'ST-119B11EA', 'SI-ECD52CD9', 'GR-250FBD0C',
  'AB-FCFAB1A9', 'RU-9F1136B4', 'ME-9940FF73', 'CL-DA786EF6'
);

/* Retrieving the locations with the amount of stores*/

SELECT locality as locality, 
        COUNT(store_code) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7

/*
QUERY RESULTS: 
"locality"	"total_no_stores"
"Chapletown"	14
"Belper"	    13
"Exeter"	    11
"Bushey"	    10
"Arbroath"	    10
"High Wycombe"	10
"Rutherglen"	 9
*/