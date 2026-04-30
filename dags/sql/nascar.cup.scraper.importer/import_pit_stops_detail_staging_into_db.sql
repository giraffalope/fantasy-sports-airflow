DELETE ps FROM nascar.pitstops ps
INNER JOIN nascar.races r ON r.id = ps.race_id
WHERE r.week_id = {{ week_id }};

---

INSERT INTO nascar.pitstops (
    race_id,
    car_driver_id,
    leader_lap,
    driver_lap,
    type,
    crew_time,
    driver_time
)
SELECT
    ranked.race_id,
    ranked.car_driver_id,
    ranked.leader_lap,
    ranked.driver_lap,
    ranked.type,
    ranked.crew_time,
    ranked.driver_time
FROM (
    SELECT
        r.id AS race_id,
        COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) AS car_driver_id,
        CAST(stg.leader_lap AS UNSIGNED) AS leader_lap,
        CAST(stg.pitter_lap AS UNSIGNED) AS driver_lap,
        stg.type AS type,
        COALESCE(CAST(stg.stop_duration AS TIME(3)), CAST('00:00:00' AS TIME(3))) AS crew_time,
        COALESCE(CAST(stg.driver_time AS TIME(3)), CAST('00:00:00' AS TIME(3))) AS driver_time,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id), CAST(stg.leader_lap AS UNSIGNED), CAST(stg.pitter_lap AS UNSIGNED)
            ORDER BY COALESCE(TRIM(stg.stop_duration), ''), COALESCE(TRIM(stg.driver_name), ''), COALESCE(TRIM(stg.number), '')
        ) AS rn
    FROM nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
        ON stg.number = tc_num.car_number
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
        ON tc_num.car_driver_id IS NULL
        AND stg.driver_name = tc_driver.driver_name
    WHERE stg.leader_lap IS NOT NULL
        AND stg.pitter_lap IS NOT NULL
        AND COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) IS NOT NULL
) AS ranked
WHERE ranked.rn = 1
;
