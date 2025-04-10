WITH WeatherData AS (
    SELECT *,
	Avg(`Temperature`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Temperature Avg`,
	Min(`Temperature`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Temperature Min`,
	Max(`Temperature`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Temperature Max`,
	LAG(`Temperature`, 1, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lag 1`,
	LEAD(`Temperature`, 1, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lead 1`,
	LAG(`Temperature`, 2, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lag 2`,
	LEAD(`Temperature`, 2, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lead 2`,
	LAG(`Temperature`, 3, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lag 3`,
	LEAD(`Temperature`, 3, 17.2) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Temperature Lead 3`,

	Avg(`Dew Point`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Dew Point Avg`,
	Min(`Dew Point`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Dew Point Min`,
	Max(`Dew Point`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Dew Point Max`,
	LAG(`Dew Point`, 1, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lag 1`,
	LEAD(`Dew Point`, 1, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lead 1`,
	LAG(`Dew Point`, 2, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lag 2`,
	LEAD(`Dew Point`, 2, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lead 2`,
	LAG(`Dew Point`, 3, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lag 3`,
	LEAD(`Dew Point`, 3, 8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Dew Point Lead 3`,

	Avg(`Humidity`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Humidity Avg`,
	Min(`Humidity`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Humidity Min`,
	Max(`Humidity`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Humidity Max`,
	LAG(`Humidity`, 1, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lag 1`,
	LEAD(`Humidity`, 1, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lead 1`,
	LAG(`Humidity`, 2, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lag 2`,
	LEAD(`Humidity`, 2, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lead 2`,
	LAG(`Humidity`, 3, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lag 3`,
	LEAD(`Humidity`, 3, 60.9) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Humidity Lead 3`,

	Sum(`Precipitation`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Precipitation Sum`,
	Max(`Precipitation`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Precipitation Max`,
	LAG(`Precipitation`, 1, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lag 1`,
	LEAD(`Precipitation`, 1, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lead 1`,
	LAG(`Precipitation`, 2, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lag 2`,
	LEAD(`Precipitation`, 2, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lead 2`,
	LAG(`Precipitation`, 3, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lag 3`,
	LEAD(`Precipitation`, 3, 0.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Precipitation Lead 3`,

	Avg(`Wind Speed`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Wind Speed Avg`,
	Max(`Wind Speed`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Wind Speed Max`,
	LAG(`Wind Speed`, 1, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lag 1`,
	LEAD(`Wind Speed`, 1, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lead 1`,
	LAG(`Wind Speed`, 2, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lag 2`,
	LEAD(`Wind Speed`, 2, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lead 2`,
	LAG(`Wind Speed`, 3, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lag 3`,
	LEAD(`Wind Speed`, 3, 13.8) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Wind Speed Lead 3`,

	Avg(`Pressure`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Pressure Avg`,
	Min(`Pressure`) OVER(PARTITION BY `Airport` ORDER BY Timestamp ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS `Pressure Min`,
	LAG(`Pressure`, 1, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lag 1`,
	LEAD(`Pressure`, 1, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lead 1`,
	LAG(`Pressure`, 2, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lag 2`,
	LEAD(`Pressure`, 2, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lead 2`,
	LAG(`Pressure`, 3, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lag 3`,
	LEAD(`Pressure`, 3, 1014.1) OVER(PARTITION BY `Airport` ORDER BY Timestamp) AS `Pressure Lead 3`
    FROM Weather
    WHERE Timestamp BETWEEN '2024-01-01 00:00:00' AND '2024-01-31 23:59:59'
),
FlightData AS (
    SELECT *, 
        TO_TIMESTAMP_NTZ(DATE_TRUNC('hour', `Scheduled Departure Timestamp`)) AS `Departure Timestamp`,
        TO_TIMESTAMP_NTZ(DATE_TRUNC('hour', `Scheduled Arrival Timestamp`)) AS `Arrival Timestamp`
    FROM Flight
)
SELECT
    f.*,
    w1.`Temperature` AS `Departure Temperature`, w2.`Temperature` AS `Arrival Temperature`,
    w1.`Dew Point` AS `Departure Dew Point`, w2.`Dew Point` AS `Arrival Dew Point`,
    w1.`Humidity` AS `Departure Humidity`, w2.`Humidity` AS `Arrival Humidity`,
    w1.`Precipitation` AS `Departure Precipitation`, w2.`Precipitation` AS `Arrival Precipitation`,
    w1.`Wind Speed` AS `Departure Wind Speed`, w2.`Wind Speed` AS `Arrival Wind Speed`,
    w1.`Pressure` AS `Departure Pressure`, w2.`Pressure` AS `Arrival Pressure`,
    w1.`Weather Code` AS `Departure Weather Code`, w2.`Weather Code` AS `Arrival Weather Code`,
    w1.`Weather Condition` AS `Departure Weather Condition`, w2.`Weather Condition` AS `Arrival Weather Condition`,
    w1.`Extreme Weather` AS `Departure Extreme Weather`, w2.`Extreme Weather` AS `Arrival Extreme Weather`,
    w1.`Temperature Avg` AS `Departure Temperature Avg`, w2.`Temperature Avg` AS `Arrival Temperature Avg`,
    w1.`Temperature Min` AS `Departure Temperature Min`, w2.`Temperature Min` AS `Arrival Temperature Min`,
    w1.`Temperature Max` AS `Departure Temperature Max`, w2.`Temperature Max` AS `Arrival Temperature Max`,
    w1.`Temperature Lag 1` AS `Departure Temperature Lag 1`, w2.`Temperature Lag 1` AS `Arrival Temperature Lag 1`,
    w1.`Temperature Lead 1` AS `Departure Temperature Lead 1`, w2.`Temperature Lead 1` AS `Arrival Temperature Lead 1`,
    w1.`Temperature Lag 2` AS `Departure Temperature Lag 2`, w2.`Temperature Lag 2` AS `Arrival Temperature Lag 2`,
    w1.`Temperature Lead 2` AS `Departure Temperature Lead 2`, w2.`Temperature Lead 2` AS `Arrival Temperature Lead 2`,
    w1.`Temperature Lag 3` AS `Departure Temperature Lag 3`, w2.`Temperature Lag 3` AS `Arrival Temperature Lag 3`,
    w1.`Temperature Lead 3` AS `Departure Temperature Lead 3`, w2.`Temperature Lead 3` AS `Arrival Temperature Lead 3`,
    w1.`Dew Point Avg` AS `Departure Dew Point Avg`, w2.`Dew Point Avg` AS `Arrival Dew Point Avg`,
    w1.`Dew Point Min` AS `Departure Dew Point Min`, w2.`Dew Point Min` AS `Arrival Dew Point Min`,
    w1.`Dew Point Max` AS `Departure Dew Point Max`, w2.`Dew Point Max` AS `Arrival Dew Point Max`,
    w1.`Dew Point Lag 1` AS `Departure Dew Point Lag 1`, w2.`Dew Point Lag 1` AS `Arrival Dew Point Lag 1`,
    w1.`Dew Point Lead 1` AS `Departure Dew Point Lead 1`, w2.`Dew Point Lead 1` AS `Arrival Dew Point Lead 1`,
    w1.`Dew Point Lag 2` AS `Departure Dew Point Lag 2`, w2.`Dew Point Lag 2` AS `Arrival Dew Point Lag 2`,
    w1.`Dew Point Lead 2` AS `Departure Dew Point Lead 2`, w2.`Dew Point Lead 2` AS `Arrival Dew Point Lead 2`,
    w1.`Dew Point Lag 3` AS `Departure Dew Point Lag 3`, w2.`Dew Point Lag 3` AS `Arrival Dew Point Lag 3`,
    w1.`Dew Point Lead 3` AS `Departure Dew Point Lead 3`, w2.`Dew Point Lead 3` AS `Arrival Dew Point Lead 3`,
    w1.`Humidity Avg` AS `Departure Humidity Avg`, w2.`Humidity Avg` AS `Arrival Humidity Avg`,
    w1.`Humidity Min` AS `Departure Humidity Min`, w2.`Humidity Min` AS `Arrival Humidity Min`,
    w1.`Humidity Max` AS `Departure Humidity Max`, w2.`Humidity Max` AS `Arrival Humidity Max`,
    w1.`Humidity Lag 1` AS `Departure Humidity Lag 1`, w2.`Humidity Lag 1` AS `Arrival Humidity Lag 1`,
    w1.`Humidity Lead 1` AS `Departure Humidity Lead 1`, w2.`Humidity Lead 1` AS `Arrival Humidity Lead 1`,
    w1.`Humidity Lag 2` AS `Departure Humidity Lag 2`, w2.`Humidity Lag 2` AS `Arrival Humidity Lag 2`,
    w1.`Humidity Lead 2` AS `Departure Humidity Lead 2`, w2.`Humidity Lead 2` AS `Arrival Humidity Lead 2`,
    w1.`Humidity Lag 3` AS `Departure Humidity Lag 3`, w2.`Humidity Lag 3` AS `Arrival Humidity Lag 3`,
    w1.`Humidity Lead 3` AS `Departure Humidity Lead 3`, w2.`Humidity Lead 3` AS `Arrival Humidity Lead 3`,
    w1.`Precipitation Sum` AS `Departure Precipitation Sum`, w2.`Precipitation Sum` AS `Arrival Precipitation Sum`,
    w1.`Precipitation Max` AS `Departure Precipitation Max`, w2.`Precipitation Max` AS `Arrival Precipitation Max`,
    w1.`Precipitation Lag 1` AS `Departure Precipitation Lag 1`, w2.`Precipitation Lag 1` AS `Arrival Precipitation Lag 1`,
    w1.`Precipitation Lead 1` AS `Departure Precipitation Lead 1`, w2.`Precipitation Lead 1` AS `Arrival Precipitation Lead 1`,
    w1.`Precipitation Lag 2` AS `Departure Precipitation Lag 2`, w2.`Precipitation Lag 2` AS `Arrival Precipitation Lag 2`,
    w1.`Precipitation Lead 2` AS `Departure Precipitation Lead 2`, w2.`Precipitation Lead 2` AS `Arrival Precipitation Lead 2`,
    w1.`Precipitation Lag 3` AS `Departure Precipitation Lag 3`, w2.`Precipitation Lag 3` AS `Arrival Precipitation Lag 3`,
    w1.`Precipitation Lead 3` AS `Departure Precipitation Lead 3`, w2.`Precipitation Lead 3` AS `Arrival Precipitation Lead 3`,
    w1.`Wind Speed Avg` AS `Departure Wind Speed Avg`, w2.`Wind Speed Avg` AS `Arrival Wind Speed Avg`,
    w1.`Wind Speed Max` AS `Departure Wind Speed Max`, w2.`Wind Speed Max` AS `Arrival Wind Speed Max`,
    w1.`Wind Speed Lag 1` AS `Departure Wind Speed Lag 1`, w2.`Wind Speed Lag 1` AS `Arrival Wind Speed Lag 1`,
    w1.`Wind Speed Lead 1` AS `Departure Wind Speed Lead 1`, w2.`Wind Speed Lead 1` AS `Arrival Wind Speed Lead 1`,
    w1.`Wind Speed Lag 2` AS `Departure Wind Speed Lag 2`, w2.`Wind Speed Lag 2` AS `Arrival Wind Speed Lag 2`,
    w1.`Wind Speed Lead 2` AS `Departure Wind Speed Lead 2`, w2.`Wind Speed Lead 2` AS `Arrival Wind Speed Lead 2`,
    w1.`Wind Speed Lag 3` AS `Departure Wind Speed Lag 3`, w2.`Wind Speed Lag 3` AS `Arrival Wind Speed Lag 3`,
    w1.`Wind Speed Lead 3` AS `Departure Wind Speed Lead 3`, w2.`Wind Speed Lead 3` AS `Arrival Wind Speed Lead 3`,
    w1.`Pressure Avg` AS `Departure Pressure Avg`, w2.`Pressure Avg` AS `Arrival Pressure Avg`,
    w1.`Pressure Min` AS `Departure Pressure Min`, w2.`Pressure Min` AS `Arrival Pressure Min`,
    w1.`Pressure Lag 1` AS `Departure Pressure Lag 1`, w2.`Pressure Lag 1` AS `Arrival Pressure Lag 1`,
    w1.`Pressure Lead 1` AS `Departure Pressure Lead 1`, w2.`Pressure Lead 1` AS `Arrival Pressure Lead 1`,
    w1.`Pressure Lag 2` AS `Departure Pressure Lag 2`, w2.`Pressure Lag 2` AS `Arrival Pressure Lag 2`,
    w1.`Pressure Lead 2` AS `Departure Pressure Lead 2`, w2.`Pressure Lead 2` AS `Arrival Pressure Lead 2`,
    w1.`Pressure Lag 3` AS `Departure Pressure Lag 3`, w2.`Pressure Lag 3` AS `Arrival Pressure Lag 3`,
    w1.`Pressure Lead 3` AS `Departure Pressure Lead 3`, w2.`Pressure Lead 3` AS `Arrival Pressure Lead 3`
FROM FlightData f 
    LEFT JOIN WeatherData w1 
        ON f.`Departure Timestamp` = w1.`Timestamp` AND f.`Origin Airport` = w1.`Airport`
    LEFT JOIN WeatherData w2 
        ON f.`Arrival Timestamp` = w2.`Timestamp` AND f.`Destination Airport` = w2.`Airport`
    WHERE w1.`Temperature` IS NOT NULL OR w2.`Temperature` IS NOT NULL
