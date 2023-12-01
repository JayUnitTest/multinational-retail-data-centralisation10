SELECT dim_date_times.month, 
		ROUND(SUM(orders_table.product_quantity::NUMERIC*dim_products.product_price::NUMERIC), 2) AS total_sales
FROM orders_table
	JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY SUM(orders_table.product_quantity*dim_products.product_price) DESC LIMIT 6;

/*

RESULT: 

"month"	"total_sales"
"08"	659523.95
"01"	654822.00
"10"	641440.50
"05"	635840.43
"03"	632428.52
"07"	630555.12

*/