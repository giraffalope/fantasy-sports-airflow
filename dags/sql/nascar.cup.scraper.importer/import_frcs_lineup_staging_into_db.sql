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

INSERT INTO nascar.race_results (race_id, car_driver_id, finish_position)
SELECT
    r.id AS race_id,
    cd.id AS car_driver_id,
    CAST(stg.start AS UNSIGNED) AS finish_position
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
JOIN nascar.cars_drivers cd
    ON cd.car_id = c.id
    AND cd.driver_id = dsd.driver_id
    AND cd.deleted_at IS NULL
JOIN nascar.races r
    ON r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
WHERE stg.start IS NOT NULL
ON DUPLICATE KEY UPDATE
    finish_position = VALUES(finish_position),
    deleted_at = NULL
;
