WITH cte 
     AS (SELECT Extract(hour FROM Cast(timestamp AS TIME))   AS Hour, 
                Extract(minute FROM Cast(timestamp AS TIME)) AS Minute, 
                Extract(second FROM Cast(timestamp AS TIME)) AS Seconds, 
                day, 
                month, 
                year, 
                date_uuid 
         FROM   dim_date_times), 
     new_timestamp 
     AS (SELECT cte.year, 
                Make_timestamp(Cast(cte.year AS INT), Cast(cte.month AS INT), 
                Cast( 
                cte.day AS INT), Cast(cte.hour AS INT), Cast(cte.minute AS INT), 
                Cast(cte.seconds AS FLOAT)) AS combined_timestamp, 
                cte.date_uuid 
         FROM   cte), 
     time_difference 
     AS (SELECT new_timestamp.year, 
                Lead(new_timestamp.combined_timestamp) 
                  OVER ( 
                    ORDER BY new_timestamp.combined_timestamp) - 
                new_timestamp.combined_timestamp AS difference 
         FROM   dim_date_times 
                JOIN new_timestamp 
                  ON dim_date_times.date_uuid = new_timestamp.date_uuid), 
     yearly_average 
     AS (SELECT time_difference.year, 
                Avg(difference) AS year_average 
         FROM   time_difference 
         GROUP  BY year 
         ORDER  BY year_average DESC) 
SELECT yearly_average.year, 
       Concat('Hours: ', Extract(hour FROM year_average), ' ', 'Minutes: ', 
       Extract( 
       minute FROM year_average), ' ', 'Seconds: ', Extract( 
       second FROM year_average), 
       ' ') AS actual_time_taken 
FROM   yearly_average; 

/*Havent done milliseconds yet*/