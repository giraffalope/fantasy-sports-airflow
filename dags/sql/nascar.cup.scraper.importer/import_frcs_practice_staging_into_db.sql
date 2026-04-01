DROP TABLE IF EXISTS nascar.temp_practice_{{ week_id }}_{{ number }};

---

DELETE FROM nascar.practices
WHERE week_id = {{ week_id }} AND number = {{ number }};

---

CREATE TABLE nascar.temp_practice_{{ week_id }}_{{ number }} (
    rank VARCHAR(255),
    driver_name VARCHAR(255),
    number VARCHAR(255),
    manufacturer VARCHAR(255),
    time VARCHAR(255),
    speed VARCHAR(255),
    num_laps VARCHAR(255),
    best_lap VARCHAR(255),
    week_id BIGINT UNSIGNED,
    driver_id BIGINT UNSIGNED,
    manufacturer_id BIGINT UNSIGNED,
    team_id BIGINT UNSIGNED,
    car_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    practice_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

INSERT INTO nascar.temp_practice_{{ week_id }}_{{ number }}
    (rank, driver_name, number, manufacturer, time, speed, num_laps, best_lap)
SELECT rank, driver_name, number, manufacturer, time, speed, num_laps, best_lap
FROM nascar.stg_frcs_practice_{{ week_id }}_{{ number }};

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }}
SET week_id = {{ week_id }};

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} p
JOIN nascar.temp_entrylist_{{ week_id }} e
    ON TRIM(p.number) = TRIM(e.number)
SET
    p.team_id = e.team_id,
    p.manufacturer = COALESCE(NULLIF(TRIM(e.manufacturer), ''), p.manufacturer);

---

INSERT INTO nascar.data_sources_drivers (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(t.driver_name)
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.data_sources ds ON ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
WHERE TRIM(t.driver_name) IS NOT NULL AND TRIM(t.driver_name) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.drivers (first_name, last_name, suffix)
SELECT x.first_name, x.last_name, x.suffix
FROM (
    SELECT DISTINCT
        nascar.get_driver_first_name(dsd.name) AS first_name,
        nascar.get_driver_last_name(dsd.name) AS last_name,
        nascar.get_driver_suffix(dsd.name) AS suffix
    FROM nascar.data_sources_drivers dsd
    JOIN nascar.data_sources ds ON ds.id = dsd.data_source_id AND ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
    WHERE dsd.driver_id IS NULL
) x
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.drivers d
    WHERE d.deleted_at IS NULL
      AND (d.first_name COLLATE utf8mb4_unicode_ci) <=> (x.first_name COLLATE utf8mb4_unicode_ci)
      AND (d.last_name COLLATE utf8mb4_unicode_ci) <=> (x.last_name COLLATE utf8mb4_unicode_ci)
      AND (d.suffix COLLATE utf8mb4_unicode_ci) <=> (x.suffix COLLATE utf8mb4_unicode_ci)
);

---

UPDATE nascar.data_sources_drivers dsd
JOIN nascar.data_sources ds ON ds.id = dsd.data_source_id AND ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
JOIN nascar.drivers d
    ON (d.first_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_first_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.last_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_last_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.suffix COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_suffix(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND d.deleted_at IS NULL
SET dsd.driver_id = d.id
WHERE dsd.driver_id IS NULL;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.data_sources_drivers dsd
    ON TRIM(t.driver_name) = TRIM(dsd.name)
    AND dsd.driver_id IS NOT NULL
    AND dsd.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
SET t.driver_id = dsd.driver_id;

---

INSERT INTO nascar.data_sources_manufacturers (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(t.manufacturer)
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.data_sources ds ON ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
WHERE TRIM(t.manufacturer) IS NOT NULL AND TRIM(t.manufacturer) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.manufacturers (series_id, name)
SELECT DISTINCT {{ series_id }}, TRIM(dsm.name)
FROM nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds ON ds.id = dsm.data_source_id AND ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
WHERE dsm.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds ON ds.id = dsm.data_source_id AND ds.name = '{{ source_name }}' AND ds.deleted_at IS NULL
JOIN nascar.manufacturers m ON TRIM(m.name) = TRIM(dsm.name) AND m.deleted_at IS NULL
SET dsm.manufacturer_id = m.id
WHERE dsm.manufacturer_id IS NULL;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.data_sources_manufacturers dsm
    ON TRIM(t.manufacturer) = TRIM(dsm.name)
    AND dsm.manufacturer_id IS NOT NULL
    AND dsm.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
SET t.manufacturer_id = dsm.manufacturer_id;

---

INSERT INTO nascar.cars (team_id, manufacturer_id, number)
SELECT DISTINCT t.team_id, t.manufacturer_id, TRIM(t.number)
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE t.team_id IS NOT NULL AND t.manufacturer_id IS NOT NULL AND TRIM(t.number) IS NOT NULL AND TRIM(t.number) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
LEFT JOIN nascar.cars c
    ON c.team_id = t.team_id
    AND c.manufacturer_id = t.manufacturer_id
    AND c.number = TRIM(t.number)
    AND c.deleted_at IS NULL
SET t.car_id = c.id;

---

INSERT INTO nascar.cars_drivers (car_id, driver_id)
SELECT DISTINCT t.car_id, t.driver_id
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE t.car_id IS NOT NULL AND t.driver_id IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id
    AND cd.driver_id = t.driver_id
    AND cd.deleted_at IS NULL
SET t.car_driver_id = cd.id;

---

INSERT INTO nascar.practices (week_id, number)
VALUES ({{ week_id }}, {{ number }});

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.practices p
    ON p.week_id = t.week_id
    AND p.number = {{ number }}
    AND p.deleted_at IS NULL
SET t.practice_id = p.id;

---

DELETE pr FROM nascar.practice_results pr
INNER JOIN nascar.practices p ON p.id = pr.practice_id
WHERE p.week_id = {{ week_id }}
  AND p.number = {{ number }};

---

INSERT INTO nascar.practice_results (practice_id, car_driver_id, finish_position, best_speed, best_time, total_laps)
SELECT
    by_rank.practice_id,
    by_rank.car_driver_id,
    by_rank.finish_position,
    by_rank.speed,
    by_rank.best_time,
    by_rank.num_laps
FROM (
    SELECT
        by_driver.practice_id,
        by_driver.car_driver_id,
        by_driver.finish_position,
        by_driver.speed,
        by_driver.best_time,
        by_driver.num_laps,
        ROW_NUMBER() OVER (
            PARTITION BY by_driver.practice_id, by_driver.finish_position
            ORDER BY by_driver.car_driver_id
        ) AS rn_rank
    FROM (
        SELECT
            t.practice_id,
            t.car_driver_id,
            CAST(TRIM(t.rank) AS UNSIGNED) AS finish_position,
            t.speed,
            CASE
                WHEN TRIM(t.time) LIKE '%:%' THEN CAST(TRIM(t.time) AS TIME(3))
                WHEN TRIM(t.time) REGEXP '^[0-9]+(\\.[0-9]*)?$' THEN CAST(
                    SEC_TO_TIME(CAST(TRIM(t.time) AS DECIMAL(12, 3))) AS TIME(3)
                )
                ELSE NULL
            END AS best_time,
            t.num_laps,
            ROW_NUMBER() OVER (
                PARTITION BY t.practice_id, t.car_driver_id
                ORDER BY CAST(TRIM(t.rank) AS UNSIGNED), TRIM(t.number)
            ) AS rn_driver
        FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
        WHERE t.time IS NOT NULL
          AND TRIM(t.time) NOT IN ('', '0.0')
          AND t.practice_id IS NOT NULL
          AND t.car_driver_id IS NOT NULL
    ) by_driver
    WHERE by_driver.rn_driver = 1
) by_rank
WHERE by_rank.rn_rank = 1;
