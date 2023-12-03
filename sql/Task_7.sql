/*What is our staff headcount?*/

SELECT SUM(dim_store_details.staff_numbers) as total_staff_numbers,
		dim_store_details.country_code AS country_code
FROM dim_store_details
GROUP BY dim_store_details.country_code 
ORDER BY total_staff_numbers DESC


/*RESULT

"total_staff_numbers"	"country_code"
        12969	              "GB"
        6085	              "DE"
        1253	              "US"

*/