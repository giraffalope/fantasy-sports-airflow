DROP TEMPORARY TABLE IF EXISTS tmp_source_ids_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} AS
SELECT
    MAX(CASE WHEN ds.name = '{{ frcs_source_name }}' THEN ds.id END) AS frcs_source_id,
    MAX(CASE WHEN ds.name = '{{ racing_reference_source_name }}' THEN ds.id END) AS rr_source_id
FROM nascar.data_sources ds
WHERE ds.deleted_at IS NULL
  AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}');

---

DROP TEMPORARY TABLE IF EXISTS tmp_current_week_season_context_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_current_week_season_context_{{ season_id }}_{{ table_suffix }} AS
SELECT
    w.season_id,
    w.number AS current_week_number
FROM nascar.weeks w
WHERE w.id = {{ week_id }}
  AND w.deleted_at IS NULL
  AND w.season_id = {{ season_id }};

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_historical_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_historical_season_{{ season_id }}_{{ table_suffix }} (
    week_id BIGINT UNSIGNED NOT NULL,
    car_number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO tmp_stg_car_historical_season_{{ season_id }}_{{ table_suffix }} (
    week_id,
    car_number,
    driver_name,
    manufacturer_name,
    team_name
)
SELECT
    fact.week_id,
    nascar.clean_car_number(c.number) AS car_number,
    nascar.clean_driver_name(TRIM(CONCAT_WS(' ', d.first_name, d.last_name, d.suffix))) AS driver_name,
    nascar.clean_manufacturer_name(m.name) AS manufacturer_name,
    nascar.clean_team_name(t.name) AS team_name
FROM (
    SELECT p.week_id, pr.car_driver_id
    FROM nascar.practice_results pr
    JOIN nascar.practices p
        ON p.id = pr.practice_id
       AND p.deleted_at IS NULL
    WHERE pr.car_driver_id IS NOT NULL
      AND pr.deleted_at IS NULL
    UNION ALL
    SELECT q.week_id, qr.car_driver_id
    FROM nascar.qualifying_results qr
    JOIN nascar.qualifyings q
        ON q.id = qr.qualifying_id
       AND q.deleted_at IS NULL
    WHERE qr.car_driver_id IS NOT NULL
      AND qr.deleted_at IS NULL
    UNION ALL
    SELECT r.week_id, rr.car_driver_id
    FROM nascar.race_results rr
    JOIN nascar.races r
        ON r.id = rr.race_id
       AND r.deleted_at IS NULL
    WHERE rr.car_driver_id IS NOT NULL
      AND rr.deleted_at IS NULL
    UNION ALL
    SELECT r.week_id, ps.car_driver_id
    FROM nascar.pitstops ps
    JOIN nascar.races r
        ON r.id = ps.race_id
       AND r.deleted_at IS NULL
    WHERE ps.car_driver_id IS NOT NULL
      AND ps.deleted_at IS NULL
    UNION ALL
    SELECT r.week_id, p.car_driver_id
    FROM nascar.penalties p
    JOIN nascar.races r
        ON r.id = p.race_id
       AND r.deleted_at IS NULL
    WHERE p.car_driver_id IS NOT NULL
      AND p.deleted_at IS NULL
    UNION ALL
    SELECT r.week_id, dsal.car_driver_id
    FROM nascar.dfs_salaries dsal
    JOIN nascar.races r
        ON r.id = dsal.race_id
       AND r.deleted_at IS NULL
    WHERE dsal.car_driver_id IS NOT NULL
      AND dsal.deleted_at IS NULL
) fact
JOIN nascar.weeks w
    ON w.id = fact.week_id
   AND w.deleted_at IS NULL
JOIN tmp_current_week_season_context_{{ season_id }}_{{ table_suffix }} ctx
    ON ctx.season_id = w.season_id
   AND w.number < ctx.current_week_number
JOIN nascar.cars_drivers cd
    ON cd.id = fact.car_driver_id
   AND cd.deleted_at IS NULL
JOIN nascar.cars c
    ON c.id = cd.car_id
   AND c.deleted_at IS NULL
JOIN nascar.drivers d
    ON d.id = cd.driver_id
   AND d.deleted_at IS NULL
LEFT JOIN nascar.manufacturers m
    ON m.id = c.manufacturer_id
   AND m.deleted_at IS NULL
   AND m.series_id = {{ series_id }}
LEFT JOIN nascar.teams t
    ON t.id = c.team_id
   AND t.deleted_at IS NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }} AS
SELECT
    nascar.clean_car_number(sc.car_number) AS rr_car_number,
    nascar.clean_driver_name(sc.driver_name) AS rr_driver_name,
    nascar.clean_manufacturer_name(sc.manufacturer_name) AS rr_manufacturer_name,
    nascar.clean_team_name(sc.team_name) AS rr_team_name
FROM nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.rr_source_id
WHERE nascar.clean_car_number(sc.car_number) IS NOT NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_numbers_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_numbers_season_{{ season_id }}_{{ table_suffix }} AS
SELECT DISTINCT rr.rr_car_number AS car_number
FROM tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }} rr
UNION
SELECT DISTINCT nascar.clean_car_number(sc.car_number) AS car_number
FROM nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.frcs_source_id
WHERE nascar.clean_car_number(sc.car_number) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }} rr
      WHERE rr.rr_car_number = nascar.clean_car_number(sc.car_number)
  );

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_frcs_to_rr_scored_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_frcs_to_rr_scored_season_{{ season_id }}_{{ table_suffix }} AS
SELECT
    nascar.clean_car_number(f.car_number) AS frcs_car_number,
    nascar.clean_driver_name(f.driver_name) AS frcs_driver_name,
    nascar.clean_manufacturer_name(f.manufacturer_name) AS frcs_manufacturer_name,
    nascar.clean_team_name(f.team_name) AS frcs_team_name,
    rr.rr_car_number,
    (
        CASE WHEN nascar.clean_driver_name(f.driver_name) IS NOT NULL
                  AND nascar.clean_driver_name(f.driver_name) = rr.rr_driver_name THEN 100 ELSE 0 END +
        CASE WHEN nascar.clean_team_name(f.team_name) IS NOT NULL
                  AND nascar.clean_team_name(f.team_name) = rr.rr_team_name THEN 10 ELSE 0 END +
        CASE WHEN nascar.clean_manufacturer_name(f.manufacturer_name) IS NOT NULL
                  AND nascar.clean_manufacturer_name(f.manufacturer_name) = rr.rr_manufacturer_name THEN 10 ELSE 0 END +
        CASE WHEN nascar.clean_car_number(f.car_number) IS NOT NULL
                  AND nascar.clean_car_number(f.car_number) = rr.rr_car_number THEN 3 ELSE 0 END
    ) AS match_score
FROM nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} f
JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src
    ON f.data_source_id = src.frcs_source_id
JOIN tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }} rr
    ON TRUE;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_frcs_mapped_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_frcs_mapped_season_{{ season_id }}_{{ table_suffix }} AS
SELECT
    picked.frcs_car_number,
    picked.frcs_driver_name,
    picked.frcs_manufacturer_name,
    picked.frcs_team_name,
    picked.rr_car_number AS canonical_car_number
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.frcs_car_number, s.frcs_driver_name, s.frcs_manufacturer_name, s.frcs_team_name
            ORDER BY s.match_score DESC, s.rr_car_number
        ) AS rn
    FROM tmp_stg_car_frcs_to_rr_scored_season_{{ season_id }}_{{ table_suffix }} s
) picked
WHERE picked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_observations_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_observations_season_{{ season_id }}_{{ table_suffix }} AS
SELECT
    rr.rr_car_number AS car_number,
    rr.rr_driver_name AS driver_name,
    rr.rr_manufacturer_name AS manufacturer_name,
    rr.rr_team_name AS team_name,
    src.rr_source_id AS data_source_id
FROM tmp_stg_car_rr_rows_season_{{ season_id }}_{{ table_suffix }} rr
JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src
    ON TRUE
UNION ALL
SELECT
    COALESCE(
        fm.canonical_car_number,
        CASE
            WHEN n.car_number IS NOT NULL THEN nascar.clean_car_number(sc.car_number)
            ELSE NULL
        END
    ) AS car_number,
    nascar.clean_driver_name(sc.driver_name) AS driver_name,
    nascar.clean_manufacturer_name(sc.manufacturer_name) AS manufacturer_name,
    nascar.clean_team_name(sc.team_name) AS team_name,
    src.frcs_source_id AS data_source_id
FROM nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.frcs_source_id
LEFT JOIN tmp_stg_car_frcs_mapped_season_{{ season_id }}_{{ table_suffix }} fm
    ON fm.frcs_car_number <=> nascar.clean_car_number(sc.car_number)
   AND fm.frcs_driver_name <=> nascar.clean_driver_name(sc.driver_name)
   AND fm.frcs_manufacturer_name <=> nascar.clean_manufacturer_name(sc.manufacturer_name)
   AND fm.frcs_team_name <=> nascar.clean_team_name(sc.team_name)
LEFT JOIN tmp_stg_car_numbers_season_{{ season_id }}_{{ table_suffix }} n
    ON n.car_number = nascar.clean_car_number(sc.car_number)
WHERE COALESCE(
    fm.canonical_car_number,
    CASE WHEN n.car_number IS NOT NULL THEN nascar.clean_car_number(sc.car_number) END
) IS NOT NULL
UNION ALL
SELECT
    nascar.clean_car_number(h.car_number) AS car_number,
    nascar.clean_driver_name(h.driver_name) AS driver_name,
    nascar.clean_manufacturer_name(h.manufacturer_name) AS manufacturer_name,
    nascar.clean_team_name(h.team_name) AS team_name,
    NULL AS data_source_id
FROM tmp_stg_car_historical_season_{{ season_id }}_{{ table_suffix }} h
WHERE nascar.clean_car_number(h.car_number) IS NOT NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_driver_rank_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_driver_rank_season_{{ season_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.driver_name
FROM (
    SELECT
        o.car_number,
        o.driver_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.driver_name
        ) AS rn
    FROM tmp_stg_car_observations_season_{{ season_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.driver_name IS NOT NULL
    GROUP BY o.car_number, o.driver_name
) ranked
WHERE ranked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_manufacturer_rank_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_manufacturer_rank_season_{{ season_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.manufacturer_name
FROM (
    SELECT
        o.car_number,
        o.manufacturer_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.manufacturer_name
        ) AS rn
    FROM tmp_stg_car_observations_season_{{ season_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.manufacturer_name IS NOT NULL
    GROUP BY o.car_number, o.manufacturer_name
) ranked
WHERE ranked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_team_rank_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_team_rank_season_{{ season_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.team_name
FROM (
    SELECT
        o.car_number,
        o.team_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.team_name
        ) AS rn
    FROM tmp_stg_car_observations_season_{{ season_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_season_{{ season_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.team_name IS NOT NULL
    GROUP BY o.car_number, o.team_name
) ranked
WHERE ranked.rn = 1;

---

DROP TABLE IF EXISTS nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }} (
    car_number VARCHAR(255) NOT NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    driver_id BIGINT UNSIGNED NULL,
    manufacturer_id BIGINT UNSIGNED NULL,
    team_id BIGINT UNSIGNED NULL,
    car_id BIGINT UNSIGNED NULL,
    car_driver_id BIGINT UNSIGNED NULL,
    PRIMARY KEY (car_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }} (
    car_number,
    driver_name,
    manufacturer_name,
    team_name
)
SELECT
    n.car_number,
    d.driver_name,
    m.manufacturer_name,
    t.team_name
FROM tmp_stg_car_numbers_season_{{ season_id }}_{{ table_suffix }} n
LEFT JOIN tmp_car_driver_rank_season_{{ season_id }}_{{ table_suffix }} d
    ON d.car_number = n.car_number
LEFT JOIN tmp_car_manufacturer_rank_season_{{ season_id }}_{{ table_suffix }} m
    ON m.car_number = n.car_number
LEFT JOIN tmp_car_team_rank_season_{{ season_id }}_{{ table_suffix }} t
    ON t.car_number = n.car_number;

---

UPDATE nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }} t
LEFT JOIN nascar.drivers d
    ON d.deleted_at IS NULL
   AND d.first_name <=> nascar.get_driver_first_name(t.driver_name)
   AND d.last_name <=> nascar.get_driver_last_name(t.driver_name)
   AND d.suffix <=> nascar.get_driver_suffix(t.driver_name)
LEFT JOIN nascar.manufacturers m
    ON m.deleted_at IS NULL
   AND m.series_id = {{ series_id }}
   AND m.name = t.manufacturer_name
LEFT JOIN nascar.teams tm
    ON tm.deleted_at IS NULL
   AND tm.name = t.team_name
LEFT JOIN nascar.cars c
    ON c.deleted_at IS NULL
   AND c.number = t.car_number
   AND c.team_id <=> tm.id
   AND c.manufacturer_id <=> m.id
LEFT JOIN nascar.cars_drivers cd
    ON cd.deleted_at IS NULL
   AND cd.car_id = c.id
   AND cd.driver_id = d.id
SET
    t.driver_id = COALESCE(t.driver_id, d.id),
    t.manufacturer_id = COALESCE(t.manufacturer_id, m.id),
    t.team_id = COALESCE(tm.id, t.team_id),
    t.car_id = COALESCE(t.car_id, c.id),
    t.car_driver_id = COALESCE(t.car_driver_id, cd.id);

---

DROP TEMPORARY TABLE IF EXISTS tmp_source_ids_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_source_ids_{{ week_id }}_{{ table_suffix }} AS
SELECT
    MAX(CASE WHEN ds.name = '{{ frcs_source_name }}' THEN ds.id END) AS frcs_source_id,
    MAX(CASE WHEN ds.name = '{{ racing_reference_source_name }}' THEN ds.id END) AS rr_source_id
FROM nascar.data_sources ds
WHERE ds.deleted_at IS NULL
  AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}');

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }} AS
SELECT
    nascar.clean_car_number(sc.car_number) AS rr_car_number,
    nascar.clean_driver_name(sc.driver_name) AS rr_driver_name,
    nascar.clean_manufacturer_name(sc.manufacturer_name) AS rr_manufacturer_name,
    nascar.clean_team_name(sc.team_name) AS rr_team_name
FROM nascar.stg_car_{{ week_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.rr_source_id
WHERE nascar.clean_car_number(sc.car_number) IS NOT NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_numbers_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_numbers_{{ week_id }}_{{ table_suffix }} AS
SELECT DISTINCT rr.rr_car_number AS car_number
FROM tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }} rr
UNION
SELECT DISTINCT nascar.clean_car_number(sc.car_number) AS car_number
FROM nascar.stg_car_{{ week_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.frcs_source_id
WHERE nascar.clean_car_number(sc.car_number) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }} rr
      WHERE rr.rr_car_number = nascar.clean_car_number(sc.car_number)
  );

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_frcs_to_rr_scored_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_frcs_to_rr_scored_{{ week_id }}_{{ table_suffix }} AS
SELECT
    nascar.clean_car_number(f.car_number) AS frcs_car_number,
    nascar.clean_driver_name(f.driver_name) AS frcs_driver_name,
    nascar.clean_manufacturer_name(f.manufacturer_name) AS frcs_manufacturer_name,
    nascar.clean_team_name(f.team_name) AS frcs_team_name,
    rr.rr_car_number,
    (
        CASE WHEN nascar.clean_driver_name(f.driver_name) IS NOT NULL
                  AND nascar.clean_driver_name(f.driver_name) = rr.rr_driver_name THEN 100 ELSE 0 END +
        CASE WHEN nascar.clean_team_name(f.team_name) IS NOT NULL
                  AND nascar.clean_team_name(f.team_name) = rr.rr_team_name THEN 10 ELSE 0 END +
        CASE WHEN nascar.clean_manufacturer_name(f.manufacturer_name) IS NOT NULL
                  AND nascar.clean_manufacturer_name(f.manufacturer_name) = rr.rr_manufacturer_name THEN 10 ELSE 0 END +
        CASE WHEN nascar.clean_car_number(f.car_number) IS NOT NULL
                  AND nascar.clean_car_number(f.car_number) = rr.rr_car_number THEN 3 ELSE 0 END
    ) AS match_score
FROM nascar.stg_car_{{ week_id }}_{{ table_suffix }} f
JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src
    ON f.data_source_id = src.frcs_source_id
JOIN tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }} rr
    ON TRUE;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_frcs_mapped_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_frcs_mapped_{{ week_id }}_{{ table_suffix }} AS
SELECT
    picked.frcs_car_number,
    picked.frcs_driver_name,
    picked.frcs_manufacturer_name,
    picked.frcs_team_name,
    picked.rr_car_number AS canonical_car_number
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.frcs_car_number, s.frcs_driver_name, s.frcs_manufacturer_name, s.frcs_team_name
            ORDER BY s.match_score DESC, s.rr_car_number
        ) AS rn
    FROM tmp_stg_car_frcs_to_rr_scored_{{ week_id }}_{{ table_suffix }} s
) picked
WHERE picked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }} AS
SELECT
    rr.rr_car_number AS car_number,
    rr.rr_driver_name AS driver_name,
    rr.rr_manufacturer_name AS manufacturer_name,
    rr.rr_team_name AS team_name,
    src.rr_source_id AS data_source_id
FROM tmp_stg_car_rr_rows_{{ week_id }}_{{ table_suffix }} rr
JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src
    ON TRUE
UNION ALL
SELECT
    COALESCE(
        fm.canonical_car_number,
        CASE
            WHEN n.car_number IS NOT NULL THEN nascar.clean_car_number(sc.car_number)
            ELSE NULL
        END
    ) AS car_number,
    nascar.clean_driver_name(sc.driver_name) AS driver_name,
    nascar.clean_manufacturer_name(sc.manufacturer_name) AS manufacturer_name,
    nascar.clean_team_name(sc.team_name) AS team_name,
    src.frcs_source_id AS data_source_id
FROM nascar.stg_car_{{ week_id }}_{{ table_suffix }} sc
JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src
    ON sc.data_source_id = src.frcs_source_id
LEFT JOIN tmp_stg_car_frcs_mapped_{{ week_id }}_{{ table_suffix }} fm
    ON fm.frcs_car_number <=> nascar.clean_car_number(sc.car_number)
   AND fm.frcs_driver_name <=> nascar.clean_driver_name(sc.driver_name)
   AND fm.frcs_manufacturer_name <=> nascar.clean_manufacturer_name(sc.manufacturer_name)
   AND fm.frcs_team_name <=> nascar.clean_team_name(sc.team_name)
LEFT JOIN tmp_stg_car_numbers_{{ week_id }}_{{ table_suffix }} n
    ON n.car_number = nascar.clean_car_number(sc.car_number)
WHERE COALESCE(
    fm.canonical_car_number,
    CASE WHEN n.car_number IS NOT NULL THEN nascar.clean_car_number(sc.car_number) END
) IS NOT NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_driver_rank_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_driver_rank_{{ week_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.driver_name
FROM (
    SELECT
        o.car_number,
        o.driver_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.driver_name
        ) AS rn
    FROM tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.driver_name IS NOT NULL
    GROUP BY o.car_number, o.driver_name
) ranked
WHERE ranked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_manufacturer_rank_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_manufacturer_rank_{{ week_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.manufacturer_name
FROM (
    SELECT
        o.car_number,
        o.manufacturer_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.manufacturer_name
        ) AS rn
    FROM tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.manufacturer_name IS NOT NULL
    GROUP BY o.car_number, o.manufacturer_name
) ranked
WHERE ranked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_team_rank_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_team_rank_{{ week_id }}_{{ table_suffix }} AS
SELECT ranked.car_number, ranked.team_name
FROM (
    SELECT
        o.car_number,
        o.team_name,
        COUNT(*) AS c,
        MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) AS has_frcs,
        ROW_NUMBER() OVER (
            PARTITION BY o.car_number
            ORDER BY COUNT(*) DESC, MAX(CASE WHEN o.data_source_id = src.frcs_source_id THEN 1 ELSE 0 END) DESC, o.team_name
        ) AS rn
    FROM tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }} o
    JOIN tmp_source_ids_{{ week_id }}_{{ table_suffix }} src ON TRUE
    WHERE o.team_name IS NOT NULL
    GROUP BY o.car_number, o.team_name
) ranked
WHERE ranked.rn = 1;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_team_source_stats_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_team_source_stats_{{ week_id }}_{{ table_suffix }} AS
SELECT
    o.data_source_id,
    COUNT(*) AS total_observations,
    SUM(CASE WHEN o.team_name IS NULL THEN 1 ELSE 0 END) AS null_team_observations
FROM tmp_stg_car_observations_{{ week_id }}_{{ table_suffix }} o
GROUP BY o.data_source_id;

---

DROP TABLE IF EXISTS nascar.tmp_car_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} (
    car_number VARCHAR(255) NOT NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    driver_id BIGINT UNSIGNED NULL,
    manufacturer_id BIGINT UNSIGNED NULL,
    team_id BIGINT UNSIGNED NULL,
    car_id BIGINT UNSIGNED NULL,
    car_driver_id BIGINT UNSIGNED NULL,
    PRIMARY KEY (car_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_car_{{ week_id }}_{{ table_suffix }} (
    car_number,
    driver_name,
    manufacturer_name,
    team_name
)
SELECT
    n.car_number,
    d.driver_name,
    m.manufacturer_name,
    t.team_name
FROM tmp_stg_car_numbers_{{ week_id }}_{{ table_suffix }} n
LEFT JOIN tmp_car_driver_rank_{{ week_id }}_{{ table_suffix }} d
    ON d.car_number = n.car_number
LEFT JOIN tmp_car_manufacturer_rank_{{ week_id }}_{{ table_suffix }} m
    ON m.car_number = n.car_number
LEFT JOIN tmp_car_team_rank_{{ week_id }}_{{ table_suffix }} t
    ON t.car_number = n.car_number;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} w
JOIN nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }} s
    ON s.car_number = w.car_number
SET
    w.driver_name = COALESCE(w.driver_name, s.driver_name),
    w.manufacturer_name = COALESCE(w.manufacturer_name, s.manufacturer_name),
    w.team_name = COALESCE(w.team_name, s.team_name),
    w.driver_id = COALESCE(w.driver_id, s.driver_id),
    w.manufacturer_id = COALESCE(w.manufacturer_id, s.manufacturer_id);

---

INSERT INTO nascar.data_sources_drivers (data_source_id, name)
SELECT DISTINCT ds.id, t.driver_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.data_sources ds
    ON ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
WHERE t.driver_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.data_sources_manufacturers (data_source_id, name)
SELECT DISTINCT ds.id, t.manufacturer_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.data_sources ds
    ON ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
WHERE t.manufacturer_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.data_sources_teams (data_source_id, name)
SELECT DISTINCT ds.id, t.team_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.data_sources ds
    ON ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
WHERE t.team_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.drivers d
    ON d.deleted_at IS NULL
   AND d.first_name <=> nascar.get_driver_first_name(t.driver_name)
   AND d.last_name <=> nascar.get_driver_last_name(t.driver_name)
   AND d.suffix <=> nascar.get_driver_suffix(t.driver_name)
LEFT JOIN nascar.manufacturers m
    ON m.deleted_at IS NULL
   AND m.series_id = {{ series_id }}
   AND m.name = t.manufacturer_name
LEFT JOIN nascar.teams tm
    ON tm.deleted_at IS NULL
   AND tm.name = t.team_name
SET
    t.driver_id = COALESCE(t.driver_id, d.id),
    t.manufacturer_id = COALESCE(t.manufacturer_id, m.id),
    t.team_id = COALESCE(tm.id, t.team_id);

---

INSERT INTO nascar.drivers (first_name, last_name, suffix)
SELECT DISTINCT
    nascar.get_driver_first_name(t.driver_name),
    nascar.get_driver_last_name(t.driver_name),
    nascar.get_driver_suffix(t.driver_name)
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.driver_name IS NOT NULL
  AND t.driver_id IS NULL
  AND nascar.get_driver_first_name(t.driver_name) IS NOT NULL
  AND nascar.get_driver_last_name(t.driver_name) IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.manufacturers (series_id, name)
SELECT DISTINCT {{ series_id }}, t.manufacturer_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.manufacturer_name IS NOT NULL
  AND t.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.teams (series_id, name)
SELECT DISTINCT {{ series_id }}, t.team_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.team_name IS NOT NULL
  AND t.team_id IS NULL
  AND TRIM(t.team_name) <> ''
  AND UPPER(TRIM(t.team_name)) <> 'N/A'
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.drivers d
    ON d.deleted_at IS NULL
   AND d.first_name <=> nascar.get_driver_first_name(t.driver_name)
   AND d.last_name <=> nascar.get_driver_last_name(t.driver_name)
   AND d.suffix <=> nascar.get_driver_suffix(t.driver_name)
LEFT JOIN nascar.manufacturers m
    ON m.deleted_at IS NULL
   AND m.series_id = {{ series_id }}
   AND m.name = t.manufacturer_name
LEFT JOIN nascar.teams tm
    ON tm.deleted_at IS NULL
   AND tm.name = t.team_name
SET
    t.driver_id = COALESCE(t.driver_id, d.id),
    t.manufacturer_id = COALESCE(t.manufacturer_id, m.id),
    t.team_id = COALESCE(tm.id, t.team_id);

---

UPDATE nascar.data_sources_drivers dsd
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
   AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
JOIN nascar.drivers d
    ON d.first_name <=> nascar.get_driver_first_name(nascar.clean_driver_name(dsd.name))
   AND d.last_name <=> nascar.get_driver_last_name(nascar.clean_driver_name(dsd.name))
   AND d.suffix <=> nascar.get_driver_suffix(nascar.clean_driver_name(dsd.name))
   AND d.deleted_at IS NULL
SET dsd.driver_id = d.id
WHERE dsd.driver_id IS NULL;

---

UPDATE nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
   AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
JOIN nascar.manufacturers m
    ON m.series_id = {{ series_id }}
   AND m.name = nascar.clean_manufacturer_name(dsm.name)
   AND m.deleted_at IS NULL
SET dsm.manufacturer_id = m.id
WHERE dsm.manufacturer_id IS NULL;

---

UPDATE nascar.data_sources_teams dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
   AND ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')
   AND ds.deleted_at IS NULL
JOIN nascar.teams tm
    ON tm.name = nascar.clean_team_name(dst.name)
   AND tm.deleted_at IS NULL
SET dst.team_id = tm.id
WHERE dst.team_id IS NULL;

---

INSERT INTO nascar.cars (team_id, manufacturer_id, number)
SELECT DISTINCT t.team_id, t.manufacturer_id, t.car_number
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.car_number IS NOT NULL
  AND t.team_id IS NOT NULL
ON DUPLICATE KEY UPDATE
    team_id = COALESCE(nascar.cars.team_id, VALUES(team_id)),
    manufacturer_id = COALESCE(nascar.cars.manufacturer_id, VALUES(manufacturer_id)),
    deleted_at = NULL;

---

INSERT INTO nascar.cars (team_id, manufacturer_id, number)
SELECT DISTINCT t.team_id, t.manufacturer_id, t.car_number
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.car_number IS NOT NULL
  AND t.team_id IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM nascar.cars c
      WHERE c.deleted_at IS NULL
        AND c.number = t.car_number
        AND c.team_id IS NULL
        AND c.manufacturer_id <=> t.manufacturer_id
  );

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_candidate_rank_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_candidate_rank_{{ week_id }}_{{ table_suffix }} AS
SELECT
    ranked.car_number,
    ranked.car_id
FROM (
    SELECT
        t.car_number,
        c.id AS car_id,
        ROW_NUMBER() OVER (
            PARTITION BY t.car_number
            ORDER BY
                CASE
                    WHEN t.team_id IS NOT NULL
                     AND t.manufacturer_id IS NOT NULL
                     AND c.team_id <=> t.team_id
                     AND c.manufacturer_id <=> t.manufacturer_id THEN 400
                    WHEN t.team_id IS NOT NULL
                     AND c.team_id <=> t.team_id
                     AND (t.manufacturer_id IS NULL OR c.manufacturer_id <=> t.manufacturer_id) THEN 300
                    WHEN t.manufacturer_id IS NOT NULL
                     AND c.manufacturer_id <=> t.manufacturer_id
                     AND (t.team_id IS NULL OR c.team_id <=> t.team_id) THEN 200
                    ELSE 100
                END DESC,
                CASE WHEN c.team_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN c.manufacturer_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                c.id
        ) AS rn
    FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
    JOIN nascar.cars c
        ON c.deleted_at IS NULL
       AND c.number = t.car_number
    WHERE t.car_number IS NOT NULL
) ranked
WHERE ranked.rn = 1;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
JOIN tmp_car_candidate_rank_{{ week_id }}_{{ table_suffix }} r
    ON r.car_number = t.car_number
SET t.car_id = r.car_id
WHERE t.car_number IS NOT NULL;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.cars c
    ON c.id = t.car_id
   AND c.deleted_at IS NULL
SET
    t.team_id = COALESCE(t.team_id, c.team_id),
    t.manufacturer_id = COALESCE(t.manufacturer_id, c.manufacturer_id)
WHERE t.car_id IS NOT NULL;

---

DROP TEMPORARY TABLE IF EXISTS tmp_car_unresolved_teams_{{ week_id }}_{{ table_suffix }};

---

CREATE TEMPORARY TABLE tmp_car_unresolved_teams_{{ week_id }}_{{ table_suffix }} AS
SELECT
    t.car_number,
    t.driver_name,
    t.manufacturer_name
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.team_id IS NULL;

---

INSERT INTO nascar.cars_drivers (car_id, driver_id)
SELECT DISTINCT t.car_id, t.driver_id
FROM nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
WHERE t.car_id IS NOT NULL
  AND t.driver_id IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.deleted_at IS NULL
   AND cd.car_id = t.car_id
   AND cd.driver_id = t.driver_id
SET t.car_driver_id = COALESCE(t.car_driver_id, cd.id)
WHERE t.car_id IS NOT NULL
  AND t.driver_id IS NOT NULL;

---

UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.deleted_at IS NULL
   AND cd.car_id = t.car_id
   AND cd.driver_id = t.driver_id
SET t.car_driver_id = cd.id
WHERE t.car_id IS NOT NULL
  AND t.driver_id IS NOT NULL
  AND cd.id IS NOT NULL;
