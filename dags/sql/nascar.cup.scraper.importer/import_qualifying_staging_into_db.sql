DROP TABLE IF EXISTS nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }};

---

UPDATE nascar.qualifyings
SET deleted_at = NOW()
WHERE week_id = {{ week_id }}
  AND deleted_at IS NULL;

---

CREATE TABLE nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }} (
    rank VARCHAR(255),
    driver_name VARCHAR(255),
    number VARCHAR(255),
    time VARCHAR(255),
    speed VARCHAR(255),
    week_id BIGINT UNSIGNED,
    car_driver_id BIGINT UNSIGNED,
    qualifying_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }} (rank, driver_name, number, time, speed)
SELECT
    rank,
    driver_name,
    number,
    time,
    speed
FROM nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }};

---

UPDATE nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }}
SET week_id = {{ week_id }};

---

UPDATE nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
    ON t.number = tc_num.car_number
LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
    ON tc_num.car_driver_id IS NULL
    AND t.driver_name = tc_driver.driver_name
SET t.car_driver_id = COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id);

---

UPDATE nascar.qualifyings q
JOIN (
    SELECT id
    FROM (
        SELECT id
        FROM nascar.qualifyings
        WHERE week_id = {{ week_id }}
        ORDER BY id DESC
        LIMIT 1
    ) revived
) revived_row
    ON revived_row.id = q.id
SET q.deleted_at = NULL;

---

INSERT INTO nascar.qualifyings (week_id)
SELECT {{ week_id }}
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.qualifyings q
    WHERE q.week_id = {{ week_id }}
      AND q.deleted_at IS NULL
);

---

UPDATE nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }} t
JOIN nascar.qualifyings q
    ON q.week_id = t.week_id
    AND q.deleted_at IS NULL
SET t.qualifying_id = q.id;

---

DELETE qr FROM nascar.qualifying_results qr
INNER JOIN nascar.qualifyings q ON q.id = qr.qualifying_id
WHERE q.week_id = {{ week_id }};

---

INSERT INTO nascar.qualifying_results (qualifying_id, car_driver_id, finish_position, best_speed, best_time)
SELECT
    by_rank.qualifying_id,
    by_rank.car_driver_id,
    by_rank.finish_position,
    by_rank.speed,
    by_rank.best_time
FROM (
    SELECT
        by_driver.qualifying_id,
        by_driver.car_driver_id,
        by_driver.finish_position,
        by_driver.speed,
        by_driver.best_time,
        ROW_NUMBER() OVER (
            PARTITION BY by_driver.qualifying_id, by_driver.finish_position
            ORDER BY by_driver.car_driver_id
        ) AS rn_rank
    FROM (
        SELECT
            t.qualifying_id,
            t.car_driver_id,
            CAST(t.rank AS UNSIGNED) AS finish_position,
            CAST(t.speed AS DECIMAL(8, 3)) AS speed,
            CAST(t.time AS TIME(3)) AS best_time,
            ROW_NUMBER() OVER (
                PARTITION BY t.qualifying_id, t.car_driver_id
                ORDER BY
                    COALESCE(CAST(t.rank AS UNSIGNED), 2147483647),
                    t.number
            ) AS rn_driver
        FROM nascar.tmp_qualifying_{{ week_id }}_{{ table_suffix }} t
        WHERE t.time IS NOT NULL
          AND TRIM(t.time) != ''
          AND t.qualifying_id IS NOT NULL
          AND t.car_driver_id IS NOT NULL
    ) by_driver
    WHERE by_driver.rn_driver = 1
      AND by_driver.finish_position IS NOT NULL
      AND by_driver.best_time IS NOT NULL
      AND by_driver.speed IS NOT NULL
) by_rank
WHERE by_rank.rn_rank = 1;
