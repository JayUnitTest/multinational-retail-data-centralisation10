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
	
/* RESULT 


"year"	                     "actual_time_taken"
"2013"	       "Hours: 2 Minutes; 17 Seconds: 15 Milliseconds: 0.655442 "
"1993" 	"Hours: 2 Minutes; 15 Seconds: 40 Milliseconds: 0.129515 "
"2002"	       "Hours: 2 Minutes; 13 Seconds: 49 Milliseconds: 0.478228 "
"2008"	       "Hours: 2 Minutes; 13 Seconds: 3 Milliseconds: 0.532442 "
"2022"	       "Hours: 2 Minutes; 13 Seconds: 2 Milliseconds: 0.003698 "
"1995"	       "Hours: 2 Minutes; 12 Seconds: 59 Milliseconds: 0.084514 "
"2016"	       "Hours: 2 Minutes; 12 Seconds: 58 Milliseconds: 0.099167 "
"2011"	       "Hours: 2 Minutes; 12 Seconds: 29 Milliseconds: 0.826536 "
"2020"	       "Hours: 2 Minutes; 12 Seconds: 10 Milliseconds: 0.518667 "
"2021"	       "Hours: 2 Minutes; 11 Seconds: 48 Milliseconds: 0.370733 "
"2012"	       "Hours: 2 Minutes; 11 Seconds: 46 Milliseconds: 0.260641 "
"2009"	       "Hours: 2 Minutes; 11 Seconds: 24 Milliseconds: 0.803098 "
"1996"	       "Hours: 2 Minutes; 11 Seconds: 11 Milliseconds: 0.387674 "
"2007"	       "Hours: 2 Minutes; 11 Seconds: 7 Milliseconds: 0.991517 "
"2010"	       "Hours: 2 Minutes; 11 Seconds: 7 Milliseconds: 0.604843 "
"1999"	       "Hours: 2 Minutes; 11 Seconds: 4 Milliseconds: 0.353455 "
"2000"	       "Hours: 2 Minutes; 11 Seconds: 0 Milliseconds: 0.146796 "
"2019"	       "Hours: 2 Minutes; 10 Seconds: 45 Milliseconds: 0.120179 "
"2001"	       "Hours: 2 Minutes; 10 Seconds: 44 Milliseconds: 0.332339 "
"2018"	       "Hours: 2 Minutes; 10 Seconds: 43 Milliseconds: 0.598161 "
"1994"	       "Hours: 2 Minutes; 10 Seconds: 37 Milliseconds: 0.971898 "
"2004"	       "Hours: 2 Minutes; 10 Seconds: 29 Milliseconds: 0.233837 "
"2006"	       "Hours: 2 Minutes; 10 Seconds: 15 Milliseconds: 0.657079 "
"2014"	       "Hours: 2 Minutes; 10 Seconds: 6 Milliseconds: 0.066848 "
"1997"	       "Hours: 2 Minutes; 9 Seconds: 48 Milliseconds: 0.347923 "
"2015"	       "Hours: 2 Minutes; 9 Seconds: 44 Milliseconds: 0.443896 "
"1992"	       "Hours: 2 Minutes; 9 Seconds: 36 Milliseconds: 0.141256 "
"2005"	       "Hours: 2 Minutes; 9 Seconds: 4 Milliseconds: 0.849779 "
"2003"	       "Hours: 2 Minutes; 8 Seconds: 39 Milliseconds: 0.432141 "
"2017"	       "Hours: 2 Minutes; 8 Seconds: 38 Milliseconds: 0.187653 "
"1998"	       "Hours: 2 Minutes; 8 Seconds: 8 Milliseconds: 0.009995 "







*/