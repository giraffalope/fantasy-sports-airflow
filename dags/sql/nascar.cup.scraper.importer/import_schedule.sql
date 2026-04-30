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

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_frcs_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_cup_schedule_frcs_{{ table_suffix }} AS
SELECT
    CAST(stg.event_number AS UNSIGNED) AS event_number,
    CASE
        WHEN CAST(stg.week_number AS UNSIGNED) = 0 THEN NULL
        ELSE CAST(stg.week_number AS UNSIGNED)
    END AS week_number,
    stg.week_date AS week_date,
    TRIM(stg.week_track) AS week_track,
    stg.week_track_url AS week_track_url,
    TRIM(stg.week_race_name) AS week_race_name
FROM nascar.stg_cup_frcs_schedule_import_{{ table_suffix }} stg;

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_rr_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_cup_schedule_rr_{{ table_suffix }} AS
SELECT
    CASE
        WHEN CAST(stg.week_number AS UNSIGNED) = 0 THEN NULL
        ELSE CAST(stg.week_number AS UNSIGNED)
    END AS week_number,
    stg.week_date AS week_date,
    TRIM(stg.week_track) AS week_track,
    stg.week_track_url AS week_track_url,
    TRIM(stg.week_race_name) AS week_race_name
FROM nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }} stg;

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_rr_match_guard_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_cup_schedule_rr_match_guard_{{ table_suffix }} (
    rr_rows_must_match_frcs TINYINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_cup_schedule_rr_match_guard_{{ table_suffix }} (rr_rows_must_match_frcs)
SELECT
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM nascar.tmp_cup_schedule_rr_{{ table_suffix }} rr
            LEFT JOIN nascar.tmp_cup_schedule_frcs_{{ table_suffix }} frcs
                ON frcs.week_number = rr.week_number
            WHERE rr.week_number IS NOT NULL
              AND frcs.week_number IS NULL
        ) THEN NULL
        ELSE 1
    END;

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_merged_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_cup_schedule_merged_{{ table_suffix }} AS
SELECT
    frcs.event_number AS frcs_event_number,
    frcs.week_number AS frcs_week_number,
    frcs.week_date AS frcs_week_date,
    frcs.week_track AS frcs_week_track,
    frcs.week_track_url AS frcs_week_track_url,
    frcs.week_race_name AS frcs_week_race_name,
    rr.week_number AS rr_week_number,
    rr.week_date AS rr_week_date,
    rr.week_track AS rr_week_track,
    rr.week_track_url AS rr_week_track_url,
    rr.week_race_name AS rr_week_race_name,
    frcs.event_number AS event_number,
    COALESCE(frcs.week_number, rr.week_number) AS week_number,
    COALESCE(frcs.week_date, rr.week_date) AS week_date,
    COALESCE(NULLIF(TRIM(frcs.week_track), ''), NULLIF(TRIM(rr.week_track), '')) AS week_track_preferred,
    COALESCE(frcs.week_track_url, rr.week_track_url) AS week_track_url,
    COALESCE(NULLIF(TRIM(frcs.week_race_name), ''), NULLIF(TRIM(rr.week_race_name), '')) AS week_race_name
FROM nascar.tmp_cup_schedule_frcs_{{ table_suffix }} frcs
LEFT JOIN nascar.tmp_cup_schedule_rr_{{ table_suffix }} rr
    ON rr.week_number = frcs.week_number;

---

INSERT INTO nascar.data_sources_tracks (data_source_id, name)
SELECT DISTINCT
    ds.id AS data_source_id,
    TRIM(merged.frcs_week_track) AS name
FROM nascar.tmp_cup_schedule_merged_{{ table_suffix }} merged
JOIN nascar.data_sources ds
    ON ds.name = '{{ frcs_source_name }}'
    AND ds.deleted_at IS NULL
WHERE NULLIF(TRIM(merged.frcs_week_track), '') IS NOT NULL
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

INSERT INTO nascar.data_sources_tracks (data_source_id, name)
SELECT DISTINCT
    ds.id AS data_source_id,
    TRIM(merged.rr_week_track) AS name
FROM nascar.tmp_cup_schedule_merged_{{ table_suffix }} merged
JOIN nascar.data_sources ds
    ON ds.name = '{{ racing_reference_source_name }}'
    AND ds.deleted_at IS NULL
WHERE NULLIF(TRIM(merged.rr_week_track), '') IS NOT NULL
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

INSERT INTO nascar.tracks (name)
SELECT DISTINCT
    TRIM(merged.week_track_preferred) AS name
FROM nascar.tmp_cup_schedule_merged_{{ table_suffix }} merged
WHERE NULLIF(TRIM(merged.week_track_preferred), '') IS NOT NULL
ON DUPLICATE KEY UPDATE
    deleted_at = NULL
;

---

UPDATE nascar.data_sources_tracks dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
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

INSERT INTO nascar.weeks (season_id, track_id, number, season_number, race_name, scheduled_start, event_type)
SELECT
    s.id AS season_id,
    t.id AS track_id,
    CAST(merged.event_number AS UNSIGNED) AS number,
    CASE
        WHEN CAST(merged.frcs_week_number AS UNSIGNED) IS NULL THEN NULL
        WHEN CAST(merged.frcs_week_number AS UNSIGNED) = 0 THEN NULL
        ELSE CAST(merged.frcs_week_number AS UNSIGNED)
    END AS season_number,
    merged.week_race_name AS race_name,
    DATE_FORMAT(merged.week_date, '%Y-%m-%d') AS scheduled_start,
    CASE
        WHEN CAST(merged.frcs_week_number AS UNSIGNED) IS NULL THEN 'OTHER'
        WHEN CAST(merged.frcs_week_number AS UNSIGNED) = 0 THEN 'OTHER'
        ELSE 'REGULAR'
    END AS event_type
FROM nascar.tmp_cup_schedule_merged_{{ table_suffix }} merged
JOIN nascar.tracks t
    ON TRIM(t.name) = TRIM(merged.week_track_preferred)
    AND t.deleted_at IS NULL
JOIN nascar.seasons s
    ON s.series_id = {{ series_id }}
    AND s.year = {{ season_year }}
    AND s.deleted_at IS NULL
WHERE merged.event_number IS NOT NULL
  AND NULLIF(TRIM(merged.week_track_preferred), '') IS NOT NULL
  AND NULLIF(TRIM(merged.week_race_name), '') IS NOT NULL
  AND merged.week_date IS NOT NULL
  AND CAST(merged.event_number AS UNSIGNED) <> 0
  AND NOT EXISTS (
    SELECT 1
    FROM nascar.weeks w
    WHERE w.season_id = s.id
      AND w.track_id = t.id
      AND w.number = CAST(merged.event_number AS UNSIGNED)
      AND w.season_number <=> (
          CASE
              WHEN CAST(merged.frcs_week_number AS UNSIGNED) IS NULL THEN NULL
              WHEN CAST(merged.frcs_week_number AS UNSIGNED) = 0 THEN NULL
              ELSE CAST(merged.frcs_week_number AS UNSIGNED)
          END
      )
      AND w.deleted_at IS NULL
)
;

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_merged_{{ table_suffix }};

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_rr_match_guard_{{ table_suffix }};

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_rr_{{ table_suffix }};

---

DROP TABLE IF EXISTS nascar.tmp_cup_schedule_frcs_{{ table_suffix }};
