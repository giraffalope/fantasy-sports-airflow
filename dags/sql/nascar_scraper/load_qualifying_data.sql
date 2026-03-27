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

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.temp_entrylist_{{ week_id }} e
    ON TRIM(t.number) = TRIM(e.number)
SET t.team_id = e.team_id
;

---

INSERT IGNORE INTO nascar.drivers (rr_full_name)
SELECT TRIM(t.driver_name) AS rr_full_name
FROM nascar.temp_qualifying_{{ week_id }} t
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.drivers d
    ON d.rr_full_name = TRIM(t.driver_name)
SET
    t.driver_id = d.id
WHERE
    d.deleted_at IS NULL
;

---

INSERT IGNORE INTO nascar.manufacturers (rr_name)
SELECT TRIM(t.manufacturer) AS rr_name
FROM nascar.temp_qualifying_{{ week_id }} t
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.manufacturers m
    ON m.rr_name = TRIM(t.manufacturer)
SET
    t.manufacturer_id = m.id
WHERE
    m.deleted_at IS NULL
;

---

INSERT IGNORE INTO nascar.cars (team_id, manufacturer_id, number)
SELECT t.team_id, t.manufacturer_id, TRIM(t.number) as number
FROM nascar.temp_qualifying_{{ week_id }} t
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.cars c
    ON c.team_id = t.team_id AND c.manufacturer_id = t.manufacturer_id AND c.number = TRIM(t.number)
SET
    t.car_id = c.id
WHERE
    c.deleted_at IS NULL
;

---

INSERT IGNORE INTO nascar.cars_drivers (car_id, driver_id)
SELECT t.car_id, t.driver_id
FROM nascar.temp_qualifying_{{ week_id }} t
;

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id AND cd.driver_id = t.driver_id
SET
    t.car_driver_id = cd.id
WHERE
    cd.deleted_at IS NULL
;

---

INSERT INTO nascar.qualifyings (week_id)
VALUES ({{ week_id }});

---

UPDATE nascar.temp_qualifying_{{ week_id }} t
JOIN nascar.qualifyings q ON q.week_id = t.week_id
SET t.qualifying_id = q.id
WHERE q.deleted_at IS NULL
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
WHERE t.time IS NOT NULL AND TRIM(t.time) NOT IN ('', '0.0')
;

---

DROP TABLE IF EXISTS nascar.temp_qualifying_{{ week_id }};