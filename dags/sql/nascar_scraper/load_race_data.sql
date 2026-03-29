DROP TABLE IF EXISTS nascar.temp_race_results_{{ week_id }};

---

DROP TABLE IF EXISTS nascar.temp_race_loop_data_{{ week_id }};

---

DROP TABLE IF EXISTS nascar.temp_race_{{ week_id }};

---

-- Delete all of the qualifyings and their race_results (via ON DELETE CASCADE)
DELETE FROM nascar.races
WHERE week_id = {{ week_id }}
;

---

CREATE TABLE nascar.temp_race_results_{{ week_id }} (
    position VARCHAR(255),
    starting_position VARCHAR(255),
    number VARCHAR(255),
    name VARCHAR(255),
    sponsor VARCHAR(255),
    owner VARCHAR(255),
    car VARCHAR(255),
    laps VARCHAR(255),
    status VARCHAR(255),
    laps_led VARCHAR(255),
    points VARCHAR(255),
    playoff_points VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ race_results_csv_file_path }}'
INTO TABLE nascar.temp_race_results_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(position, starting_position, number, name, sponsor, owner, car, laps, status, laps_led, points, playoff_points)
;

---

CREATE TABLE nascar.temp_race_loop_data_{{ week_id }} (
    driver_name VARCHAR(255),
    start VARCHAR(255),
    mid_race VARCHAR(255),
    finish VARCHAR(255),
    high_pos VARCHAR(255),
    low_pos VARCHAR(255),
    avg_pos VARCHAR(255),
    pass_diff VARCHAR(255),
    green_flag_passes VARCHAR(255),
    green_flag_times_passed VARCHAR(255),
    quality_passes VARCHAR(255),
    pct_quality_passes VARCHAR(255),
    fastest_lap VARCHAR(255),
    top_15_laps VARCHAR(255),
    pct_top_15_laps VARCHAR(255),
    laps_led VARCHAR(255),
    pct_laps_led VARCHAR(255),
    total_laps VARCHAR(255),
    driver_rating VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ race_loop_data_csv_file_path }}'
INTO TABLE nascar.temp_race_loop_data_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(driver_name, start, mid_race, finish, high_pos, low_pos, avg_pos, pass_diff, green_flag_passes, green_flag_times_passed, quality_passes, pct_quality_passes, fastest_lap, top_15_laps, pct_top_15_laps, laps_led, pct_laps_led, total_laps, driver_rating)
;

---

CREATE TABLE nascar.temp_race_{{ week_id }} (
    position VARCHAR(255),
    starting_position VARCHAR(255),
    number VARCHAR(255),
    name VARCHAR(255),
    sponsor VARCHAR(255),
    owner VARCHAR(255),
    car VARCHAR(255),
    laps VARCHAR(255),
    status VARCHAR(255),
    laps_led1 VARCHAR(255),
    points VARCHAR(255),
    playoff_points VARCHAR(255),

    driver_name VARCHAR(255),
    start VARCHAR(255),
    mid_race VARCHAR(255),
    finish VARCHAR(255),
    high_pos VARCHAR(255),
    low_pos VARCHAR(255),
    avg_pos VARCHAR(255),
    pass_diff VARCHAR(255),
    green_flag_passes VARCHAR(255),
    green_flag_times_passed VARCHAR(255),
    quality_passes VARCHAR(255),
    pct_quality_passes VARCHAR(255),
    fastest_lap VARCHAR(255),
    top_15_laps VARCHAR(255),
    pct_top_15_laps VARCHAR(255),
    laps_led2 VARCHAR(255),
    pct_laps_led VARCHAR(255),
    total_laps VARCHAR(255),
    driver_rating VARCHAR(255),

    week_id BIGINT UNSIGNED,
    driver_id BIGINT UNSIGNED,
    manufacturer_id BIGINT UNSIGNED,
    team_id BIGINT UNSIGNED,
    car_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    race_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

INSERT INTO nascar.temp_race_{{ week_id }}
(
    position,
    starting_position,
    number,
    name,
    sponsor,
    owner,
    car,
    laps,
    status,
    laps_led1,
    points,
    playoff_points
)
SELECT
    position,
    starting_position,
    number,
    name,
    sponsor,
    owner,
    car,
    laps,
    status,
    laps_led,
    points,
    playoff_points
FROM nascar.temp_race_results_{{ week_id }}
;

---

UPDATE nascar.temp_race_{{ week_id }} r
JOIN nascar.temp_race_loop_data_{{ week_id }} l
    ON r.name = l.driver_name
SET
    r.driver_name = l.driver_name,
    r.start = l.start,
    r.mid_race = l.mid_race,
    r.finish = l.finish,
    r.high_pos = l.high_pos,
    r.low_pos = l.low_pos,
    r.avg_pos = l.avg_pos,
    r.pass_diff = l.pass_diff,
    r.green_flag_passes = l.green_flag_passes,
    r.green_flag_times_passed = l.green_flag_times_passed,
    r.quality_passes = l.quality_passes,
    r.pct_quality_passes = l.pct_quality_passes,
    r.fastest_lap = l.fastest_lap,
    r.top_15_laps = l.top_15_laps,
    r.pct_top_15_laps = l.pct_top_15_laps,
    r.laps_led2 = l.laps_led,
    r.pct_laps_led = l.pct_laps_led,
    r.total_laps = l.total_laps,
    r.driver_rating = l.driver_rating
;

---

UPDATE nascar.temp_race_{{ week_id }}
SET week_id = {{ week_id }}
;

---

-- Car number is stable: team and manufacturer come from the entry list for that number even if session driver differs (substitute driver, same car).
UPDATE nascar.temp_race_{{ week_id }} t
JOIN nascar.temp_entrylist_{{ week_id }} e
    ON TRIM(t.number) = TRIM(e.number)
SET
    t.team_id = e.team_id,
    t.car = COALESCE(NULLIF(TRIM(e.manufacturer), ''), t.car)
;

---

INSERT INTO nascar.racing_reference_drivers (full_name)
SELECT DISTINCT TRIM(COALESCE(t.driver_name, t.name)) AS full_name
FROM nascar.temp_race_{{ week_id }} t
WHERE TRIM(COALESCE(t.driver_name, t.name)) IS NOT NULL AND TRIM(COALESCE(t.driver_name, t.name)) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT IGNORE INTO nascar.drivers (first_name, last_name, suffix)
SELECT  nascar.get_driver_first_name(rrd.full_name) AS first_name,
        nascar.get_driver_last_name(rrd.full_name) AS last_name,
        nascar.get_driver_suffix(rrd.full_name) AS suffix
FROM nascar.temp_race_{{ week_id }} t
JOIN nascar.racing_reference_drivers rrd
    ON TRIM(COALESCE(t.driver_name, t.name)) = TRIM(rrd.full_name)
    AND rrd.deleted_at IS NULL
WHERE rrd.driver_id IS NULL
;

---

UPDATE nascar.racing_reference_drivers rrd
    JOIN nascar.temp_race_{{ week_id }} t
        ON rrd.driver_id IS NULL
        AND TRIM(COALESCE(t.driver_name, t.name)) = TRIM(rrd.full_name)
        AND rrd.deleted_at IS NULL
    JOIN nascar.drivers d
        ON d.first_name = nascar.get_driver_first_name(rrd.full_name)
        AND d.last_name = nascar.get_driver_last_name(rrd.full_name)
        AND d.suffix = nascar.get_driver_suffix(rrd.full_name)
        AND d.deleted_at IS NULL
    SET rrd.driver_id = d.id
;

---

UPDATE nascar.temp_race_{{ week_id }} t
JOIN nascar.racing_reference_drivers rrd
    ON rrd.driver_id IS NOT NULL
    AND TRIM(COALESCE(t.driver_name, t.name)) = TRIM(rrd.full_name)
    AND rrd.deleted_at IS NULL
SET
    t.driver_id = rrd.driver_id
;

---

INSERT INTO nascar.racing_reference_manufacturers (name)
SELECT DISTINCT TRIM(t.car) AS name
FROM nascar.temp_race_{{ week_id }} t
WHERE TRIM(t.car) IS NOT NULL AND TRIM(t.car) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.manufacturers (name)
SELECT DISTINCT TRIM(rrm.name) AS name
FROM nascar.racing_reference_manufacturers rrm
JOIN nascar.temp_race_{{ week_id }} t
    ON TRIM(t.car) = TRIM(rrm.name)
    AND rrm.deleted_at IS NULL
WHERE rrm.manufacturer_id IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.racing_reference_manufacturers rrm
JOIN nascar.temp_race_{{ week_id }} t
    ON TRIM(t.car) = TRIM(rrm.name)
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

UPDATE nascar.temp_race_{{ week_id }} t
JOIN nascar.racing_reference_manufacturers rrm
    ON TRIM(t.car) = TRIM(rrm.name)
    AND rrm.manufacturer_id IS NOT NULL
    AND rrm.deleted_at IS NULL
SET
    t.manufacturer_id = rrm.manufacturer_id
;

---

UPDATE nascar.temp_race_{{ week_id }} t
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
FROM nascar.temp_race_{{ week_id }} t
WHERE t.team_id IS NOT NULL
    AND t.manufacturer_id IS NOT NULL
    AND TRIM(t.number) IS NOT NULL
    AND TRIM(t.number) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_race_{{ week_id }} t
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
FROM nascar.temp_race_{{ week_id }} t
WHERE t.car_id IS NOT NULL
    AND t.driver_id IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.temp_race_{{ week_id }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id
    AND cd.driver_id = t.driver_id
    AND cd.deleted_at IS NULL
SET
    t.car_driver_id = cd.id
;

---

INSERT INTO nascar.races (week_id)
VALUES ({{ week_id }});

---

UPDATE nascar.temp_race_{{ week_id }} t
JOIN nascar.races r
    ON r.week_id = t.week_id
    AND r.deleted_at IS NULL
SET t.race_id = r.id
;

---

INSERT INTO nascar.race_results (
    race_id,
    car_driver_id,
    start_position,
    finish_position,
    best_position,
    worst_position,
    average_position,
    pass_diff,
    green_flag_passes,
    green_flag_times_passed,
    quality_passes,
    quality_passes_pct,
    total_fastest_laps,
    total_top_15_laps,
    total_top_15_laps_pct,
    total_laps_led,
    total_laps_led_pct,
    total_laps_completed,
    driver_rating,
    car_status
)
SELECT 
    t.race_id AS race_id,
    t.car_driver_id AS car_driver_id,
    COALESCE(t.start, t.starting_position) AS start_position,
    COALESCE(t.finish, t.position) AS finish_position,
    t.high_pos AS best_position,
    t.low_pos AS worst_position,
    t.avg_pos AS average_position,
    t.pass_diff AS pass_diff,
    t.green_flag_passes AS green_flag_passes,
    t.green_flag_times_passed AS green_flag_times_passed,
    t.quality_passes AS quality_passes,
    t.pct_quality_passes AS quality_passes_pct,
    t.fastest_lap AS total_fastest_laps,
    t.top_15_laps AS total_top_15_laps,
    t.pct_top_15_laps AS total_top_15_laps_pct,
    COALESCE(t.laps_led2, t.laps_led1) AS total_laps_led,
    t.pct_laps_led AS total_laps_led_pct,
    t.total_laps AS total_laps_completed,
    t.driver_rating AS driver_rating,
    t.status AS car_status
FROM nascar.temp_race_{{ week_id }} t
WHERE COALESCE(t.finish, t.position) IS NOT NULL
    AND TRIM(COALESCE(t.finish, t.position)) != ''
    AND t.race_id IS NOT NULL
    AND t.car_driver_id IS NOT NULL
;

---

DROP TABLE IF EXISTS nascar.temp_race_results_{{ week_id }};

---

DROP TABLE IF EXISTS nascar.temp_race_loop_data_{{ week_id }};

---

DROP TABLE IF EXISTS nascar.temp_race_{{ week_id }};
