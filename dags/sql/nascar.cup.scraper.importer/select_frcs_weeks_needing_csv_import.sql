SELECT w.id AS week_id, w.number AS event_number
FROM nascar.weeks w
JOIN nascar.seasons s
    ON s.id = w.season_id
    AND s.deleted_at IS NULL
LEFT JOIN nascar.races r
    ON r.week_id = w.id
    AND r.deleted_at IS NULL
LEFT JOIN nascar.race_results rr
    ON rr.race_id = r.id
    AND rr.deleted_at IS NULL
    AND rr.finish_position IS NOT NULL
    AND TRIM(CAST(rr.finish_position AS CHAR)) <> ''
WHERE s.series_id = {{ series_id }}
    AND s.year = {{ year }}
    AND w.deleted_at IS NULL
GROUP BY w.id, w.number
HAVING COUNT(rr.id) = 0
ORDER BY w.number ASC;
