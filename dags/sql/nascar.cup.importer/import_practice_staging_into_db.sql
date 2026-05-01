DROP TABLE IF EXISTS nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }};

---

DELETE FROM nascar.practices
WHERE week_id = {{ week_id }} AND number = {{ number }};

---

CREATE TABLE nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }} (
    rank VARCHAR(255),
    driver_name VARCHAR(255),
    number VARCHAR(255),
    time VARCHAR(255),
    speed VARCHAR(255),
    num_laps VARCHAR(255),
    best_lap VARCHAR(255),
    `5_lap_average_time` VARCHAR(255),
    `10_lap_average_time` VARCHAR(255),
    `20_lap_average_time` VARCHAR(255),
    `25_lap_average_time` VARCHAR(255),
    `30_lap_average_time` VARCHAR(255),
    `10_lap_average_speed` VARCHAR(255),
    `10_lap_start` VARCHAR(255),
    `10_lap_end` VARCHAR(255),
    week_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    practice_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
    (
        rank,
        driver_name,
        number,
        time,
        speed,
        num_laps,
        best_lap,
        `5_lap_average_time`,
        `10_lap_average_time`,
        `20_lap_average_time`,
        `25_lap_average_time`,
        `30_lap_average_time`,
        `10_lap_average_speed`,
        `10_lap_start`,
        `10_lap_end`
    )
SELECT
    rank,
    driver_name,
    number,
    time,
    speed,
    num_laps,
    best_lap,
    `5_lap_average_time`,
    `10_lap_average_time`,
    `20_lap_average_time`,
    `25_lap_average_time`,
    `30_lap_average_time`,
    `10_lap_average_speed`,
    `10_lap_start`,
    `10_lap_end`
FROM nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }};

---

UPDATE nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
SET week_id = {{ week_id }};

---

UPDATE nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }} p
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
    ON p.number = tc_num.car_number
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
    ON tc_num.car_driver_id IS NULL
    AND p.driver_name = tc_driver.driver_name
SET p.car_driver_id = COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id);

---

INSERT INTO nascar.practices (week_id, number)
VALUES ({{ week_id }}, {{ number }});

---

UPDATE nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }} t
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

INSERT INTO nascar.practice_results (
    practice_id,
    car_driver_id,
    finish_position,
    best_speed,
    best_time,
    total_laps,
    `5_lap_average_time`,
    `10_lap_average_time`,
    `20_lap_average_time`,
    `25_lap_average_time`,
    `30_lap_average_time`,
    `10_lap_average_speed`,
    `10_lap_start`,
    `10_lap_end`
)
SELECT
    by_rank.practice_id,
    by_rank.car_driver_id,
    by_rank.finish_position,
    by_rank.speed,
    by_rank.best_time,
    by_rank.num_laps,
    by_rank.`5_lap_average_time`,
    by_rank.`10_lap_average_time`,
    by_rank.`20_lap_average_time`,
    by_rank.`25_lap_average_time`,
    by_rank.`30_lap_average_time`,
    by_rank.`10_lap_average_speed`,
    by_rank.`10_lap_start`,
    by_rank.`10_lap_end`
FROM (
    SELECT
        by_driver.practice_id,
        by_driver.car_driver_id,
        by_driver.finish_position,
        by_driver.speed,
        by_driver.best_time,
        by_driver.num_laps,
        by_driver.`5_lap_average_time`,
        by_driver.`10_lap_average_time`,
        by_driver.`20_lap_average_time`,
        by_driver.`25_lap_average_time`,
        by_driver.`30_lap_average_time`,
        by_driver.`10_lap_average_speed`,
        by_driver.`10_lap_start`,
        by_driver.`10_lap_end`,
        ROW_NUMBER() OVER (
            PARTITION BY by_driver.practice_id, by_driver.finish_position
            ORDER BY by_driver.car_driver_id
        ) AS rn_rank
    FROM (
        SELECT
            t.practice_id,
            t.car_driver_id,
            CAST(t.rank AS UNSIGNED) AS finish_position,
            CAST(t.speed AS DECIMAL(8, 3)) AS speed,
            CAST(t.time AS TIME(3)) AS best_time,
            CAST(t.num_laps AS UNSIGNED) AS num_laps,
            CAST(t.`5_lap_average_time` AS TIME(3)) AS `5_lap_average_time`,
            CAST(t.`10_lap_average_time` AS TIME(3)) AS `10_lap_average_time`,
            CAST(t.`20_lap_average_time` AS TIME(3)) AS `20_lap_average_time`,
            CAST(t.`25_lap_average_time` AS TIME(3)) AS `25_lap_average_time`,
            CAST(t.`30_lap_average_time` AS TIME(3)) AS `30_lap_average_time`,
            CAST(t.`10_lap_average_speed` AS DECIMAL(8, 3)) AS `10_lap_average_speed`,
            CAST(t.`10_lap_start` AS UNSIGNED) AS `10_lap_start`,
            CAST(t.`10_lap_end` AS UNSIGNED) AS `10_lap_end`,
            ROW_NUMBER() OVER (
                PARTITION BY t.practice_id, t.car_driver_id
                ORDER BY CAST(t.rank AS UNSIGNED), t.number
            ) AS rn_driver
        FROM nascar.tmp_practice_{{ week_id }}_{{ number }}_{{ table_suffix }} t
        WHERE t.time IS NOT NULL
          AND TRIM(t.time) != ''
          AND t.practice_id IS NOT NULL
          AND t.car_driver_id IS NOT NULL
    ) by_driver
    WHERE by_driver.rn_driver = 1
      AND by_driver.best_time IS NOT NULL
      AND by_driver.speed IS NOT NULL
) by_rank
WHERE by_rank.rn_rank = 1;
