/*Which month in each year produced the highest cost of sales?*/

SELECT ROUND(SUM(orders_table.product_quantity::NUMERIC * dim_products.product_price::NUMERIC), 2) as total_sales,
		dim_date_times.year AS "Year", 
		dim_date_times.month AS "Month"
FROM orders_table
		JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
		JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC LIMIT 10

/*RESULT
"total_sales"	"Year"	    "Month"
27623.77	    "1994"	    "03"
26663.26	    "2019"	    "01"
26350.31	    "2009"	    "08"
26174.00	    "1997"	    "11"
26079.72	    "2019"	    "08"
25850.73	    "2017"	    "09"
25665.12	    "2010"	    "05"
25392.54	    "2000"	    "01"
25357.33	    "1996"	    "08"
25288.29	    "2018"	    "12"

*/