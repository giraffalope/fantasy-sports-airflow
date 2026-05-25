DROP TABLE IF EXISTS nascar.tmp_race_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_race_{{ week_id }}_{{ table_suffix }} (
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
    green_flag_passes VARCHAR(255),
    green_flag_times_passed VARCHAR(255),
    quality_passes VARCHAR(255),
    fastest_lap VARCHAR(255),
    top_15_laps VARCHAR(255),
    laps_led2 VARCHAR(255),
    total_laps VARCHAR(255),
    driver_rating VARCHAR(255),
    week_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    race_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_race_{{ week_id }}_{{ table_suffix }}
(
    position, starting_position, number, name, sponsor, owner, car, laps, status, laps_led1, points, playoff_points
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
FROM nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }};

---

UPDATE nascar.tmp_race_{{ week_id }}_{{ table_suffix }} r
JOIN nascar.stg_frcs_race_loop_data_{{ week_id }}_{{ table_suffix }} l
    ON TRIM(r.name) = TRIM(l.driver_name)
SET
    r.driver_name = l.driver_name,
    r.start = l.start,
    r.mid_race = l.mid_race,
    r.finish = l.finish,
    r.high_pos = l.high_pos,
    r.low_pos = l.low_pos,
    r.avg_pos = l.avg_pos,
    r.green_flag_passes = l.green_flag_passes,
    r.green_flag_times_passed = l.green_flag_times_passed,
    r.quality_passes = l.quality_passes,
    r.fastest_lap = l.fastest_lap,
    r.top_15_laps = CASE
        WHEN l.total_laps IS NOT NULL
            AND l.pct_top_15_laps IS NOT NULL
        THEN CAST(
            ROUND(
                CAST(l.total_laps AS DECIMAL(20, 6)) * CAST(l.pct_top_15_laps AS DECIMAL(20, 6)) / 100.0,
                0
            ) AS CHAR
        )
        ELSE NULL
    END,
    r.laps_led2 = l.laps_led,
    r.total_laps = l.total_laps,
    r.driver_rating = l.driver_rating;

---

UPDATE nascar.tmp_race_{{ week_id }}_{{ table_suffix }}
SET week_id = {{ week_id }};

---

UPDATE nascar.tmp_race_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
    ON t.number = tc_num.car_number
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
    ON COALESCE(t.driver_name, t.name) = tc_driver.driver_name
SET t.car_driver_id = COALESCE(tc_driver.car_driver_id, tc_num.car_driver_id);

---

INSERT INTO nascar.races (week_id)
SELECT {{ week_id }}
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.races r
    WHERE r.week_id = {{ week_id }}
      AND r.deleted_at IS NULL
);

---

UPDATE nascar.tmp_race_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.races r
    ON r.week_id = t.week_id
    AND r.deleted_at IS NULL
SET t.race_id = r.id;

---

-- Re-runs: remove prior race-only shells (no finish stats). Lineup-only rows from
-- ``import_lineup`` keep ``total_laps_completed IS NULL`` and are not deleted here.
DELETE rr
FROM nascar.race_results rr
INNER JOIN nascar.races r
    ON r.id = rr.race_id
WHERE r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
    AND rr.deleted_at IS NULL
    AND rr.total_laps_completed IS NULL
    AND rr.finish_position IS NULL
;

---

INSERT INTO nascar.race_results (
    race_id, car_driver_id, start_position, finish_position, best_position, worst_position,
    average_position, green_flag_passes, green_flag_times_passed, quality_passes,
    total_fastest_laps, total_top_15_laps, total_laps_led, total_laps_completed, driver_rating, car_status
)
SELECT
    by_finish.race_id,
    by_finish.car_driver_id,
    by_finish.start_position,
    by_finish.finish_position,
    by_finish.best_position,
    by_finish.worst_position,
    by_finish.average_position,
    by_finish.green_flag_passes,
    by_finish.green_flag_times_passed,
    by_finish.quality_passes,
    by_finish.total_fastest_laps,
    by_finish.total_top_15_laps,
    by_finish.total_laps_led,
    by_finish.total_laps_completed,
    by_finish.driver_rating,
    by_finish.car_status
FROM (
    SELECT
        by_driver.race_id,
        by_driver.car_driver_id,
        by_driver.start_position,
        by_driver.finish_position,
        by_driver.best_position,
        by_driver.worst_position,
        by_driver.average_position,
        by_driver.green_flag_passes,
        by_driver.green_flag_times_passed,
        by_driver.quality_passes,
        by_driver.total_fastest_laps,
        by_driver.total_top_15_laps,
        by_driver.total_laps_led,
        by_driver.total_laps_completed,
        by_driver.driver_rating,
        by_driver.car_status,
        ROW_NUMBER() OVER (
            PARTITION BY by_driver.race_id, TRIM(COALESCE(by_driver.finish_position, ''))
            ORDER BY by_driver.car_driver_id
        ) AS rn_finish
    FROM (
        SELECT
            t.race_id,
            t.car_driver_id,
            COALESCE(t.start, t.starting_position) AS start_position,
            COALESCE(t.finish, t.position) AS finish_position,
            t.high_pos AS best_position,
            t.low_pos AS worst_position,
            t.avg_pos AS average_position,
            t.green_flag_passes,
            t.green_flag_times_passed,
            t.quality_passes,
            t.fastest_lap AS total_fastest_laps,
            t.top_15_laps AS total_top_15_laps,
            COALESCE(t.laps_led2, t.laps_led1) AS total_laps_led,
            t.total_laps AS total_laps_completed,
            t.driver_rating,
            t.status AS car_status,
            ROW_NUMBER() OVER (
                PARTITION BY t.race_id, t.car_driver_id
                ORDER BY CAST(TRIM(COALESCE(t.finish, t.position)) AS UNSIGNED), TRIM(t.number)
            ) AS rn_driver
        FROM nascar.tmp_race_{{ week_id }}_{{ table_suffix }} t
        WHERE COALESCE(t.finish, t.position) IS NOT NULL
          AND TRIM(COALESCE(t.finish, t.position)) != ''
          AND t.race_id IS NOT NULL
          AND t.car_driver_id IS NOT NULL
    ) by_driver
    WHERE by_driver.rn_driver = 1
) by_finish
WHERE by_finish.rn_finish = 1;
