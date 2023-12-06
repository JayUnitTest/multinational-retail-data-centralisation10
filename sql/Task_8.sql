/*Which German store type is selling the most?*/

SELECT ROUND(SUM(orders_table.product_quantity::NUMERIC * dim_products.product_price::NUMERIC), 2) as total_sales,
		dim_store_details.store_type AS store_type,
		dim_store_details.country_code AS country_code
FROM orders_table
		JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
		JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales

/*RESULT
"total_sales"	"store_type"	"country_code"
198250.60	       "Outlet"	           "DE"
220283.64	       "Mall Kiosk"	       "DE"
384340.36	       "Super Store"	   "DE"
1096219.79	       "Local"	           "DE"
*/