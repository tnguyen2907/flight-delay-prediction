SELECT 
    ROUND(AVG(TotalFlights), 0) AS YearlyAvgTotalFlights,
    ROUND(AVG(TotalDepartureDelayed), 0) AS YearlyAvgTotalDepartureDelayed,
    ROUND(AVG(TotalDepartureDelayMinutes), 0) AS YearlyAvgTotalDepartureDelayMinutes
FROM (
    SELECT
        COUNT(*) AS TotalFlights,
        SUM(DepartureDelayed) AS TotalDepartureDelayed,
        SUM(DepartureDelayMinutes) AS TotalDepartureDelayMinutes
    FROM
        `{gcp_project_id}.flight_delay_pred_dataset.dashboard`
    GROUP BY
        Year
)