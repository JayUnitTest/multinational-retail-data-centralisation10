SELECT COUNT(orders_table.product_quantity) as number_of_sales,
		SUM (orders_table.product_quantity) as product_quantity_count,
		CASE 
			WHEN dim_store_details.store_code = 'WEB-1388012W' then 'Web'
			ELSE 'Offline'
			END AS location
FROM orders_table 
		JOIN dim_products on  orders_table.product_code = dim_products.product_code
		JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY SUM(orders_table.product_quantity);

/*
RESULT:

"number_of_sales"	"product_quantity_count"	"location"
26957	                     107739	               "Web"
90638	                     363716	              "Offline"

*/