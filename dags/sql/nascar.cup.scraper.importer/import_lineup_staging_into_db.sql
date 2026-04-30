UPDATE nascar.races
SET deleted_at = NOW()
WHERE week_id = {{ week_id }}
  AND deleted_at IS NULL;

---

UPDATE nascar.races r
JOIN (
    SELECT id
    FROM (
        SELECT id
        FROM nascar.races
        WHERE week_id = {{ week_id }}
        ORDER BY id DESC
        LIMIT 1
    ) revived
) revived_row
    ON revived_row.id = r.id
SET r.deleted_at = NULL;

---

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

-- Lineup re-runs: ``ON DUPLICATE KEY UPDATE`` does not dedupe two rows in the *same* INSERT that
-- share (race_id, finish_position) or (race_id, car_driver_id). Clear this week’s results first, then
-- insert a clean set (see ``rn_pos`` / ``rn_driver`` below).
DELETE rr
FROM nascar.race_results rr
INNER JOIN nascar.races r
    ON r.id = rr.race_id
WHERE r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
;

---

INSERT INTO nascar.race_results (race_id, car_driver_id, finish_position)
SELECT
    lineup_rr.race_id,
    lineup_rr.car_driver_id,
    lineup_rr.finish_position
FROM (
    SELECT
        r.id AS race_id,
        COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) AS car_driver_id,
        CAST(stg.start AS UNSIGNED) AS finish_position,
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
;
