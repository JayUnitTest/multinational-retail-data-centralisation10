/* TASK 1 How many stores does the business have and in which countries? */

SELECT dim_store_details.country_code as country,
		COUNT(dim_store_details.store_code) as total_no_stores
FROM dim_store_details
GROUP BY country

/*This returns 
"country"	"total_no_stores"
	              12
"DE"	         138
"US"	          33
"GB"	         258

From this query we can see how many stores they have in each country. However 
there are 12 stores with null values. This could be a cleaning problem here and so needs to be addressed.
*/

SELECT * 
FROM dim_store_details
WHERE country_code IS NULL

/*The results from this showed that they all have store_codes however have null values in other columns therefore this data is incomplete and can be dropped or as it is only 12 stores, 
we could chase up the store_codes and get the information missing.*/