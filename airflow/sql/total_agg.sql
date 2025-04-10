SELECT 
    MIN(FlightDate) AS MinFlightDate,
    MAX(FlightDate) AS MaxFlightDate,
    ROUND(COUNT(*)) AS TotalFlights,
    ROUND(SUM(DepartureDelayed)) AS TotalDepartureDelayed,
    ROUND(SUM(DepartureDelayMinutes)) AS TotalDepartureDelayMinutes,
    ROUND(SUM(CarrierDelayMinutes), 0) AS TotalCarrierDelayMinutes,
    ROUND(SUM(WeatherDelayMinutes), 0) AS TotalWeatherDelayMinutes,
    ROUND(SUM(NASDelayMinutes), 0) AS TotalNASDelayMinutes,
    ROUND(SUM(SecurityDelayMinutes), 0) AS TotalSecurityDelayMinutes,
    ROUND(SUM(LateAircraftDelayMinutes), 0) AS TotalLateAircraftDelayMinutes,
    (SELECT ROUND(AVG(DepartureDelayMinutes), 0) FROM `{gcp_project_id}.flight_delay_pred_dataset.dashboard` WHERE DepartureDelayed = 1) AS AvgDepartureDelayMinutes,
FROM
    `{gcp_project_id}.flight_delay_pred_dataset.dashboard`