INSERT INTO nascar.data_sources_teams (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(stg.team)
FROM nascar.stg_frcs_lineup_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE stg.team IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.teams (series_id, name)
SELECT DISTINCT {{ series_id }}, TRIM(dst.name)
FROM nascar.data_sources_teams dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE dst.team_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.data_sources_teams dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.teams t
    ON TRIM(t.name) = TRIM(dst.name)
    AND t.deleted_at IS NULL
SET dst.team_id = t.id
WHERE dst.team_id IS NULL
  AND dst.deleted_at IS NULL
;

---

INSERT INTO nascar.data_sources_manufacturers (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(stg.manufacturer)
FROM nascar.stg_frcs_lineup_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE stg.manufacturer IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.manufacturers (series_id, name)
SELECT DISTINCT {{ series_id }}, TRIM(dsm.name)
FROM nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE dsm.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.manufacturers m
    ON TRIM(m.name) = TRIM(dsm.name)
    AND m.deleted_at IS NULL
SET dsm.manufacturer_id = m.id
WHERE dsm.manufacturer_id IS NULL
  AND dsm.deleted_at IS NULL
;

---

INSERT INTO nascar.data_sources_drivers (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(stg.driver_name)
FROM nascar.stg_frcs_lineup_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE stg.driver_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.drivers (first_name, last_name, suffix)
SELECT x.first_name, x.last_name, x.suffix
FROM (
    SELECT DISTINCT
        nascar.get_driver_first_name(dsd.name) AS first_name,
        nascar.get_driver_last_name(dsd.name) AS last_name,
        nascar.get_driver_suffix(dsd.name) AS suffix
    FROM nascar.data_sources_drivers dsd
    JOIN nascar.data_sources ds
        ON ds.id = dsd.data_source_id
        AND ds.name = '{{ source_name }}'
        AND ds.deleted_at IS NULL
    WHERE dsd.driver_id IS NULL
) x
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.drivers d
    WHERE d.deleted_at IS NULL
      AND (d.first_name COLLATE utf8mb4_unicode_ci) <=> (x.first_name COLLATE utf8mb4_unicode_ci)
      AND (d.last_name COLLATE utf8mb4_unicode_ci) <=> (x.last_name COLLATE utf8mb4_unicode_ci)
      AND (d.suffix COLLATE utf8mb4_unicode_ci) <=> (x.suffix COLLATE utf8mb4_unicode_ci)
)
;

---

UPDATE nascar.data_sources_drivers dsd
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.drivers d
    ON (d.first_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_first_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.last_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_last_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.suffix COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_suffix(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND d.deleted_at IS NULL
SET dsd.driver_id = d.id
WHERE dsd.driver_id IS NULL
  AND dsd.deleted_at IS NULL
;

---

INSERT INTO nascar.cars (team_id, manufacturer_id, number)
SELECT DISTINCT
    dst.team_id,
    dsm.manufacturer_id,
    TRIM(stg.number)
FROM nascar.stg_frcs_lineup_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.data_sources_teams dst
    ON dst.data_source_id = ds.id
    AND TRIM(dst.name) = TRIM(stg.team)
    AND dst.team_id IS NOT NULL
    AND dst.deleted_at IS NULL
JOIN nascar.data_sources_manufacturers dsm
    ON dsm.data_source_id = ds.id
    AND TRIM(dsm.name) = TRIM(stg.manufacturer)
    AND dsm.manufacturer_id IS NOT NULL
    AND dsm.deleted_at IS NULL
WHERE stg.number IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.cars_drivers (car_id, driver_id)
SELECT DISTINCT
    c.id AS car_id,
    dsd.driver_id AS driver_id
FROM nascar.stg_frcs_lineup_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.data_sources_drivers dsd
    ON dsd.data_source_id = ds.id
    AND TRIM(dsd.name) = TRIM(stg.driver_name)
    AND dsd.driver_id IS NOT NULL
    AND dsd.deleted_at IS NULL
JOIN nascar.data_sources_teams dst
    ON dst.data_source_id = ds.id
    AND TRIM(dst.name) = TRIM(stg.team)
    AND dst.team_id IS NOT NULL
    AND dst.deleted_at IS NULL
JOIN nascar.data_sources_manufacturers dsm
    ON dsm.data_source_id = ds.id
    AND TRIM(dsm.name) = TRIM(stg.manufacturer)
    AND dsm.manufacturer_id IS NOT NULL
    AND dsm.deleted_at IS NULL
JOIN nascar.cars c
    ON c.team_id = dst.team_id
    AND c.manufacturer_id = dsm.manufacturer_id
    AND c.number = TRIM(stg.number)
    AND c.deleted_at IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.races (week_id)
SELECT {{ week_id }}
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.races r
    WHERE r.week_id = {{ week_id }}
      AND r.deleted_at IS NULL
)
;

---

-- Lineup re-runs: ``ON DUPLICATE KEY UPDATE`` does not dedupe two rows in the *same* INSERT that
-- share (race_id, finish_position) or (race_id, car_driver_id). Clear this week’s results first, then
-- insert a clean set (see ``rn_pos`` / ``rn_driver`` below).
DELETE rr
FROM nascar.race_results rr
INNER JOIN nascar.races r
    ON r.id = rr.race_id
WHERE r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
;

---

INSERT INTO nascar.race_results (race_id, car_driver_id, finish_position)
SELECT
    race_id,
    car_driver_id,
    finish_position
FROM (
    SELECT
        r.id AS race_id,
        cd.id AS car_driver_id,
        CAST(TRIM(stg.start) AS UNSIGNED) AS finish_position,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, CAST(TRIM(stg.start) AS UNSIGNED)
            ORDER BY cd.id, stg.driver_name, stg.number
        ) AS rn_pos,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, cd.id
            ORDER BY CAST(TRIM(stg.start) AS UNSIGNED), stg.number, stg.driver_name
        ) AS rn_driver
    FROM nascar.stg_frcs_lineup_{{ week_id }} stg
    JOIN nascar.data_sources ds
        ON ds.name = '{{ source_name }}'
        AND ds.deleted_at IS NULL
    JOIN (
        SELECT
            dsd1.id,
            dsd1.data_source_id,
            TRIM(dsd1.name) AS name_trim
        FROM nascar.data_sources_drivers dsd1
        INNER JOIN (
            SELECT data_source_id, TRIM(name) AS name_trim, MIN(id) AS min_id
            FROM nascar.data_sources_drivers
            WHERE deleted_at IS NULL
            GROUP BY data_source_id, TRIM(name)
        ) dsd_name_pick
            ON dsd_name_pick.data_source_id = dsd1.data_source_id
            AND dsd_name_pick.name_trim = TRIM(dsd1.name)
            AND dsd_name_pick.min_id = dsd1.id
        WHERE dsd1.deleted_at IS NULL
    ) dsd_pick
        ON dsd_pick.data_source_id = ds.id
        AND dsd_pick.name_trim = TRIM(stg.driver_name)
    JOIN nascar.data_sources_drivers dsd
        ON dsd.id = dsd_pick.id
        AND dsd.driver_id IS NOT NULL
    JOIN (
        SELECT
            dst1.id,
            dst1.data_source_id,
            TRIM(dst1.name) AS name_trim
        FROM nascar.data_sources_teams dst1
        INNER JOIN (
            SELECT data_source_id, TRIM(name) AS name_trim, MIN(id) AS min_id
            FROM nascar.data_sources_teams
            WHERE deleted_at IS NULL
            GROUP BY data_source_id, TRIM(name)
        ) dst_name_pick
            ON dst_name_pick.data_source_id = dst1.data_source_id
            AND dst_name_pick.name_trim = TRIM(dst1.name)
            AND dst_name_pick.min_id = dst1.id
        WHERE dst1.deleted_at IS NULL
    ) dst_pick
        ON dst_pick.data_source_id = ds.id
        AND dst_pick.name_trim = TRIM(stg.team)
    JOIN nascar.data_sources_teams dst
        ON dst.id = dst_pick.id
        AND dst.team_id IS NOT NULL
    JOIN (
        SELECT
            dsm1.id,
            dsm1.data_source_id,
            TRIM(dsm1.name) AS name_trim
        FROM nascar.data_sources_manufacturers dsm1
        INNER JOIN (
            SELECT data_source_id, TRIM(name) AS name_trim, MIN(id) AS min_id
            FROM nascar.data_sources_manufacturers
            WHERE deleted_at IS NULL
            GROUP BY data_source_id, TRIM(name)
        ) dsm_name_pick
            ON dsm_name_pick.data_source_id = dsm1.data_source_id
            AND dsm_name_pick.name_trim = TRIM(dsm1.name)
            AND dsm_name_pick.min_id = dsm1.id
        WHERE dsm1.deleted_at IS NULL
    ) dsm_pick
        ON dsm_pick.data_source_id = ds.id
        AND dsm_pick.name_trim = TRIM(stg.manufacturer)
    JOIN nascar.data_sources_manufacturers dsm
        ON dsm.id = dsm_pick.id
        AND dsm.manufacturer_id IS NOT NULL
    JOIN (
        SELECT MIN(c.id) AS id, c.team_id, c.manufacturer_id, c.number
        FROM nascar.cars c
        WHERE c.deleted_at IS NULL
        GROUP BY c.team_id, c.manufacturer_id, c.number
    ) c_pick
        ON c_pick.team_id = dst.team_id
        AND c_pick.manufacturer_id = dsm.manufacturer_id
        AND c_pick.number = TRIM(stg.number)
    JOIN nascar.cars c
        ON c.id = c_pick.id
    JOIN (
        SELECT car_id, driver_id, MIN(id) AS id
        FROM nascar.cars_drivers
        WHERE deleted_at IS NULL
        GROUP BY car_id, driver_id
    ) cd_pick
        ON cd_pick.car_id = c.id
        AND cd_pick.driver_id = dsd.driver_id
    JOIN nascar.cars_drivers cd
        ON cd.id = cd_pick.id
    JOIN nascar.races r
        ON r.id = (
            SELECT MIN(r2.id)
            FROM nascar.races r2
            WHERE r2.week_id = {{ week_id }}
                AND r2.deleted_at IS NULL
        )
    WHERE TRIM(stg.start) IS NOT NULL
        AND TRIM(stg.start) <> ''
) lineup_rr
WHERE lineup_rr.rn_pos = 1
    AND lineup_rr.rn_driver = 1
;
