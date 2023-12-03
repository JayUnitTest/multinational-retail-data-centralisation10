/*What percentage of sales come through each type of store?*/

SELECT dim_store_details.store_type as store_type,
		ROUND(SUM(orders_table.product_quantity::NUMERIC * dim_products.product_price::NUMERIC), 2) as total_sales,
		(ROUND(SUM(orders_table.product_quantity::NUMERIC * dim_products.product_price::NUMERIC * 100 )
			   / SUM(SUM(orders_table.product_quantity::NUMERIC * dim_products.product_price::NUMERIC)) OVER (), 2)) AS percentage_total
FROM orders_table
		JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
		JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY percentage_total DESC

/* RESULT 
"store_type"	"total_sales"	"percentage_total"
"Local"	        3344518.14	            44.28
"Web Portal"	1725486.26	            22.85
"Super Store"	1195786.04	            15.83
"Mall Kiosk"	671183.56	            8.89
"Outlet"	    615713.54	            8.15
*/