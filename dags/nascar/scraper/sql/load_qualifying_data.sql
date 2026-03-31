DROP TABLE IF EXISTS nascar.temp_qualifying_{{ week_id }};

---

-- Delete all of the qualifyings and their qualifying_results (via ON DELETE CASCADE)
DELETE FROM nascar.qualifyings
WHERE week_id = {{ week_id }}
;

---

CREATE TABLE nascar.temp_qualifying_{{ week_id }} (
    rank VARCHAR(255),
    driver_name VARCHAR(255),
    number VARCHAR(255),
    manufacturer VARCHAR(255),
    time VARCHAR(255),
    speed VARCHAR(255),
    week_id BIGINT UNSIGNED,
    driver_id BIGINT UNSIGNED,
    manufacturer_id BIGINT UNSIGNED,
    team_id BIGINT UNSIGNED,
    car_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    qualifying_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ qualifying_csv_file_path }}'
INTO TABLE nascar.temp_qualifying_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(rank, driver_name, number, manufacturer, time, speed)
;

---

UPDATE nascar.temp_qualifying_{{ week_id }}
SET week_id = {{ week_id }}
;

---

-- Car number is stable: team and manufacturer come from the entry list for that number even if session driver_name differs (substitute driver, same car).
UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.temp_entrylist_{{ week_id }} e
    ON TRIM(t.number) = TRIM(e.number)
SET
    t.team_id = e.team_id,
    t.manufacturer = COALESCE(NULLIF(TRIM(e.manufacturer), ''), t.manufacturer)
;

---

INSERT INTO nascar.data_sources_drivers (data_source_id, full_name)
SELECT DISTINCT ds.id AS data_source_id, TRIM(t.driver_name) AS full_name
FROM nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.data_sources ds
    ON ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
WHERE TRIM(t.driver_name) IS NOT NULL AND TRIM(t.driver_name) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT IGNORE INTO nascar.drivers (first_name, last_name, suffix)
SELECT  nascar.get_driver_first_name(rrd.full_name) AS first_name,
        nascar.get_driver_last_name(rrd.full_name) AS last_name,
        nascar.get_driver_suffix(rrd.full_name) AS suffix
FROM nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.data_sources_drivers dsd
    ON TRIM(t.driver_name) = TRIM(dsd.full_name)
    AND dsd.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
    AND ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
WHERE dsd.driver_id IS NULL
;

---

UPDATE nascar.data_sources_drivers dsd
    JOIN nascar.data_sources ds
        ON ds.id = dsd.data_source_id
        AND ds.name = 'racing_reference'
        AND ds.deleted_at IS NULL
    JOIN nascar.temp_qualifying_{{ week_id }} t
        ON dsd.driver_id IS NULL
        AND TRIM(t.driver_name) = TRIM(dsd.full_name)
        AND dsd.deleted_at IS NULL
    JOIN nascar.drivers d
        ON d.first_name = nascar.get_driver_first_name(dsd.full_name)
        AND d.last_name = nascar.get_driver_last_name(dsd.full_name)
        AND d.suffix = nascar.get_driver_suffix(dsd.full_name)
        AND d.deleted_at IS NULL
    SET dsd.driver_id = d.id
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.data_sources_drivers dsd
    ON dsd.driver_id IS NOT NULL
    AND TRIM(t.driver_name) = TRIM(dsd.full_name)
    AND dsd.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
    AND ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
SET
    t.driver_id = dsd.driver_id
;

---

INSERT INTO nascar.data_sources_manufacturers (data_source_id, name)
SELECT DISTINCT ds.id AS data_source_id, TRIM(t.manufacturer) AS name
FROM nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.data_sources ds
    ON ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
WHERE TRIM(t.manufacturer) IS NOT NULL AND TRIM(t.manufacturer) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.manufacturers (series_id, name)
SELECT DISTINCT {{ series_id }}, TRIM(dsm.name) AS name
FROM nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
JOIN nascar.temp_qualifying_{{ week_id }} t
    ON TRIM(t.manufacturer) = TRIM(dsm.name)
    AND dsm.deleted_at IS NULL
WHERE dsm.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.data_sources_manufacturers dsm
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
JOIN nascar.temp_qualifying_{{ week_id }} t
    ON TRIM(t.manufacturer) = TRIM(dsm.name)
    AND dsm.deleted_at IS NULL
JOIN nascar.manufacturers m
    ON TRIM(m.name) = TRIM(dsm.name)
    AND m.deleted_at IS NULL
SET
    dsm.manufacturer_id = m.id
WHERE
    dsm.manufacturer_id IS NULL
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.data_sources_manufacturers dsm
    ON TRIM(t.manufacturer) = TRIM(dsm.name)
    AND dsm.manufacturer_id IS NOT NULL
    AND dsm.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.id = dsm.data_source_id
    AND ds.name = 'racing_reference'
    AND ds.deleted_at IS NULL
SET
    t.manufacturer_id = dsm.manufacturer_id
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.cars c
    ON c.team_id = t.team_id
    AND c.manufacturer_id = t.manufacturer_id
    AND c.number = TRIM(t.number)
    AND c.deleted_at IS NULL
SET
    t.car_id = c.id
;

---

INSERT INTO nascar.cars (team_id, manufacturer_id, number)
SELECT DISTINCT t.team_id, t.manufacturer_id, TRIM(t.number) AS number
FROM nascar.temp_qualifying_{{ week_id }} t
WHERE t.team_id IS NOT NULL
    AND t.manufacturer_id IS NOT NULL
    AND TRIM(t.number) IS NOT NULL
    AND TRIM(t.number) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.cars c
    ON c.team_id = t.team_id
    AND c.manufacturer_id = t.manufacturer_id
    AND c.number = TRIM(t.number)
    AND c.deleted_at IS NULL
SET
    t.car_id = c.id
;

---

INSERT INTO nascar.cars_drivers (car_id, driver_id)
SELECT DISTINCT t.car_id, t.driver_id
FROM nascar.temp_qualifying_{{ week_id }} t
WHERE t.car_id IS NOT NULL
    AND t.driver_id IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id
    AND cd.driver_id = t.driver_id
    AND cd.deleted_at IS NULL
SET
    t.car_driver_id = cd.id
;

---

INSERT INTO nascar.qualifyings (week_id)
VALUES ({{ week_id }});

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.qualifyings q
    ON q.week_id = t.week_id
    AND q.deleted_at IS NULL
SET t.qualifying_id = q.id
;

---

INSERT INTO nascar.qualifying_results (
    qualifying_id,
    car_driver_id,
    finish_position,
    best_speed,
    best_time
)
SELECT 
    t.qualifying_id AS qualifying_id,
    t.car_driver_id AS car_driver_id,
    t.rank AS finish_position,
    t.speed AS best_speed,
    t.time AS best_time
FROM nascar.temp_qualifying_{{ week_id }} t
WHERE t.time IS NOT NULL
    AND TRIM(t.time) NOT IN ('', '0.0')
    AND t.qualifying_id IS NOT NULL
    AND t.car_driver_id IS NOT NULL
;

---

DROP TABLE IF EXISTS nascar.temp_qualifying_{{ week_id }};