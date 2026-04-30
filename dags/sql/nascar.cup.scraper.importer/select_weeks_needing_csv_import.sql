-- Cup weeks for this season that may have FRCS CSVs on disk.
--
-- Previously this query only returned weeks with no race_results rows, so once the race was
-- imported the week disappeared from the DAG’s XCom list and post-race files (penalties,
-- pit-stops-detail, DraftKings, FanDuel) were never staged. We return all weeks; each Python
-- task still skips when the relevant CSV is missing or the staging table already suffices.

SELECT w.id AS week_id, w.number AS event_number
FROM nascar.weeks w
JOIN nascar.seasons s
    ON s.id = w.season_id
    AND s.deleted_at IS NULL
WHERE s.series_id = {{ series_id }}
    AND s.year = {{ year }}
    AND w.deleted_at IS NULL
    AND w.number IS NOT NULL
ORDER BY w.number ASC;
