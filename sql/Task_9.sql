/*How quickly is the company making sales*/


WITH cte AS 
(
		SELECT
				EXTRACT(HOUR FROM CAST(timestamp AS time)) AS Hours,
				EXTRACT(MINUTE FROM CAST(timestamp AS time)) AS Minutes,
				EXTRACT(SECOND FROM CAST(timestamp AS time)) AS Seconds,
				day,
				month,
				year,
				date_uuid
		FROM dim_date_times
),
new_timestamp AS (
		SELECT
			MAKE_TIMESTAMP(
			CAST(cte.year AS int),
			CAST(cte.month AS int),
			CAST(cte.day AS int),
			CAST(cte.Hours AS int),
			CAST(cte.Minutes AS int),
			CAST(cte.Seconds AS float)
			) AS combined_timestamp,
			cte.year,
			cte.date_uuid
	FROM cte
),
time_difference AS 
(
		SELECT new_timestamp.year,
				LEAD(new_timestamp.combined_timestamp) OVER (ORDER BY new_timestamp.combined_timestamp) - new_timestamp.combined_timestamp AS time_diff
		FROM dim_date_times
		JOIN new_timestamp ON dim_date_times.date_uuid = new_timestamp.date_uuid
),
yearly_average AS 
(
		SELECT time_difference.year, 
				AVG(time_diff) AS year_average
		FROM time_difference
		GROUP BY time_difference.year
		ORDER BY year_average DESC

)
	SELECT yearly_average.year,
		CONCAT('Hours: ', EXTRACT(HOUR FROM year_average), ' ',
			   'Minutes; ', EXTRACT(MINUTE FROM year_average), ' ',
			   'Seconds: ', FLOOR(EXTRACT(SECOND FROM year_average)), ' ',
			   'Milliseconds: ', EXTRACT(SECOND FROM year_average) - FLOOR(EXTRACT(SECOND FROM year_average)), ' '
			  ) AS actual_time_taken
		FROM yearly_average
	