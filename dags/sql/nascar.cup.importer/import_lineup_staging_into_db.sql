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

-- Lineup re-runs: remove prior lineup-only shells (no race stats). Post-race rows from
-- ``import_races`` keep ``total_laps_completed`` and are not deleted here.
DELETE rr
FROM nascar.race_results rr
INNER JOIN nascar.races r
    ON r.id = rr.race_id
WHERE r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
    AND rr.deleted_at IS NULL
    AND rr.total_laps_completed IS NULL
;

---

INSERT INTO nascar.race_results (race_id, car_driver_id, start_position)
SELECT
    lineup_rr.race_id,
    lineup_rr.car_driver_id,
    lineup_rr.start_position
FROM (
    SELECT
        r.id AS race_id,
        COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) AS car_driver_id,
        CAST(stg.start AS UNSIGNED) AS start_position,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, CAST(stg.start AS UNSIGNED)
            ORDER BY COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id), stg.driver_name, stg.number
        ) AS rn_pos,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id)
            ORDER BY CAST(stg.start AS UNSIGNED), stg.number, stg.driver_name
        ) AS rn_driver
    FROM nascar.stg_frcs_lineup_{{ week_id }}_{{ table_suffix }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
        ON stg.number = tc_num.car_number
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
        ON tc_num.car_driver_id IS NULL
        AND stg.driver_name = tc_driver.driver_name
    WHERE stg.start IS NOT NULL
        AND stg.start <> ''
        AND COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) IS NOT NULL
) lineup_rr
WHERE lineup_rr.rn_pos = 1
    AND lineup_rr.rn_driver = 1
ON DUPLICATE KEY UPDATE
    start_position = VALUES(start_position)
;
