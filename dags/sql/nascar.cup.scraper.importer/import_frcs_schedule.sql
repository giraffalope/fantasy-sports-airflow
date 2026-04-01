INSERT INTO nascar.data_sources (name, url)
VALUES ('{{ source_name }}', '{{ source_url }}')
ON DUPLICATE KEY UPDATE
    url = VALUES(url),
    deleted_at = NULL
;

---

INSERT INTO nascar.series (id, name)
VALUES ({{ series_id }}, '{{ series_name }}')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    deleted_at = NULL
;

---

INSERT INTO nascar.seasons (series_id, year)
VALUES ({{ series_id }}, {{ season_year }})
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

INSERT INTO nascar.data_sources_tracks (data_source_id, name)
SELECT DISTINCT
    ds.id AS data_source_id,
    TRIM(stg.week_track) AS name
FROM nascar.stg_cup_frcs_schedule_import_{{ table_suffix }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE TRIM(stg.week_track) <> ''
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

INSERT INTO nascar.tracks (name)
SELECT DISTINCT
    TRIM(dst.name) AS name
FROM nascar.data_sources_tracks dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE dst.deleted_at IS NULL
  AND dst.track_id IS NULL
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

UPDATE nascar.data_sources_tracks dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.tracks t
    ON TRIM(t.name) = TRIM(dst.name)
    AND t.deleted_at IS NULL
SET
    dst.track_id = t.id,
    dst.deleted_at = NULL
WHERE dst.deleted_at IS NULL
  AND dst.track_id IS NULL
;

---

INSERT INTO nascar.weeks (season_id, track_id, number, season_number, race_name, scheduled_start)
SELECT
    s.id AS season_id,
    dst.track_id AS track_id,
    CAST(stg.event_number AS UNSIGNED) AS number,
    CASE
        WHEN CAST(stg.week_number AS UNSIGNED) = 0 THEN NULL
        ELSE CAST(stg.week_number AS UNSIGNED)
    END AS season_number,
    stg.week_race_name AS race_name,
    DATE_FORMAT(stg.week_date, '%Y-%m-%d') AS scheduled_start
FROM nascar.stg_cup_frcs_schedule_import_{{ table_suffix }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.data_sources_tracks dst
    ON dst.data_source_id = ds.id
    AND TRIM(dst.name) = TRIM(stg.week_track)
    AND dst.track_id IS NOT NULL
    AND dst.deleted_at IS NULL
JOIN nascar.seasons s
    ON s.series_id = {{ series_id }}
    AND s.year = {{ season_year }}
    AND s.deleted_at IS NULL
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.weeks w
    WHERE w.season_id = s.id
      AND w.track_id = dst.track_id
      AND w.number = CAST(stg.event_number AS UNSIGNED)
      AND w.season_number <=> (
          CASE
              WHEN CAST(stg.week_number AS UNSIGNED) = 0 THEN NULL
              ELSE CAST(stg.week_number AS UNSIGNED)
          END
      )
      AND w.deleted_at IS NULL
)
;
