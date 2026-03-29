DROP TABLE IF EXISTS nascar.temp_practice_{{ week_id }}_{{ number }};

---

-- Delete all of the practices and their practice_results (via ON DELETE CASCADE)
DELETE FROM nascar.practices
WHERE week_id = {{ week_id }} and number = {{ number }}
;

---

CREATE TABLE nascar.temp_practice_{{ week_id }}_{{ number }} (
    rank VARCHAR(255),
    driver_name VARCHAR(255),
    number VARCHAR(255),
    manufacturer VARCHAR(255),
    time VARCHAR(255),
    time_diff VARCHAR(255),
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

LOAD DATA LOCAL INFILE '{{ practice_csv_file_path }}'
INTO TABLE nascar.temp_practice_{{ week_id }}_{{ number }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(rank, driver_name, number, manufacturer, time, time_diff, speed, num_laps, best_lap)
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }}
SET week_id = {{ week_id }}
;

---

-- Car number is stable: team and manufacturer come from the entry list for that number even if session driver_name differs (substitute driver, same car).
UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} p
JOIN nascar.temp_entrylist_{{ week_id }} e
    ON TRIM(p.number) = TRIM(e.number)
SET
    p.team_id = e.team_id,
    p.manufacturer = COALESCE(NULLIF(TRIM(e.manufacturer), ''), p.manufacturer)
;

---

INSERT INTO nascar.racing_reference_drivers (full_name)
SELECT DISTINCT TRIM(t.driver_name) AS full_name
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE TRIM(t.driver_name) IS NOT NULL AND TRIM(t.driver_name) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT IGNORE INTO nascar.drivers (first_name, last_name, suffix)
SELECT  nascar.get_driver_first_name(rrd.full_name) AS first_name,
        nascar.get_driver_last_name(rrd.full_name) AS last_name,
        nascar.get_driver_suffix(rrd.full_name) AS suffix
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.racing_reference_drivers rrd
    ON TRIM(t.driver_name) = TRIM(rrd.full_name)
    AND rrd.deleted_at IS NULL
WHERE rrd.driver_id IS NULL
;

---

UPDATE nascar.racing_reference_drivers rrd
    JOIN nascar.temp_practice_{{ week_id }}_{{ number }} t
        ON rrd.driver_id IS NULL
        AND TRIM(t.driver_name) = TRIM(rrd.full_name)
        AND rrd.deleted_at IS NULL
    JOIN nascar.drivers d
        ON d.first_name = nascar.get_driver_first_name(rrd.full_name)
        AND d.last_name = nascar.get_driver_last_name(rrd.full_name)
        AND d.suffix = nascar.get_driver_suffix(rrd.full_name)
        AND d.deleted_at IS NULL
    SET rrd.driver_id = d.id
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.racing_reference_drivers rrd
    ON rrd.driver_id IS NOT NULL
    AND TRIM(t.driver_name) = TRIM(rrd.full_name)
    AND rrd.deleted_at IS NULL
SET
    t.driver_id = rrd.driver_id
;

---

INSERT INTO nascar.racing_reference_manufacturers (name)
SELECT DISTINCT TRIM(t.manufacturer) AS name
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE TRIM(t.manufacturer) IS NOT NULL AND TRIM(t.manufacturer) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.manufacturers (name)
SELECT DISTINCT TRIM(rrm.name) AS name
FROM nascar.racing_reference_manufacturers rrm
JOIN nascar.temp_practice_{{ week_id }}_{{ number }} t
    ON TRIM(t.manufacturer) = TRIM(rrm.name)
    AND rrm.deleted_at IS NULL
WHERE rrm.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.racing_reference_manufacturers rrm
JOIN nascar.temp_practice_{{ week_id }}_{{ number }} t
    ON TRIM(t.manufacturer) = TRIM(rrm.name)
    AND rrm.deleted_at IS NULL
JOIN nascar.manufacturers m
    ON TRIM(m.name) = TRIM(rrm.name)
    AND m.deleted_at IS NULL
SET
    rrm.manufacturer_id = m.id
WHERE
    rrm.manufacturer_id IS NULL
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.racing_reference_manufacturers rrm
    ON TRIM(t.manufacturer) = TRIM(rrm.name)
    AND rrm.manufacturer_id IS NOT NULL
    AND rrm.deleted_at IS NULL
SET
    t.manufacturer_id = rrm.manufacturer_id
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
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
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE t.team_id IS NOT NULL
    AND t.manufacturer_id IS NOT NULL
    AND TRIM(t.number) IS NOT NULL
    AND TRIM(t.number) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
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
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE t.car_id IS NOT NULL
    AND t.driver_id IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id
    AND cd.driver_id = t.driver_id
    AND cd.deleted_at IS NULL
SET
    t.car_driver_id = cd.id
;

---

INSERT INTO nascar.practices (week_id, number)
VALUES ({{ week_id }}, {{ number }});

---

UPDATE nascar.temp_practice_{{ week_id }}_{{ number }} t
JOIN nascar.practices p
    ON p.week_id = t.week_id
    AND p.number = {{ number }}
    AND p.deleted_at IS NULL
SET t.practice_id = p.id
;

---

INSERT IGNORE INTO nascar.practice_results (
    practice_id,
    car_driver_id,
    finish_position,
    best_speed,
    best_time,
    total_laps
)
SELECT 
    t.practice_id AS practice_id,
    t.car_driver_id AS car_driver_id,
    t.rank AS finish_position,
    t.speed AS best_speed,
    t.time AS best_time,
    t.num_laps AS total_laps
FROM nascar.temp_practice_{{ week_id }}_{{ number }} t
WHERE t.time IS NOT NULL
    AND TRIM(t.time) NOT IN ('', '0.0')
    AND t.practice_id IS NOT NULL
    AND t.car_driver_id IS NOT NULL
;

---

DROP TABLE IF EXISTS nascar.temp_practice_{{ week_id }}_{{ number }};