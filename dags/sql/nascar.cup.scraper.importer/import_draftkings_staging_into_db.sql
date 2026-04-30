INSERT INTO nascar.dfs (name, url)
VALUES ('DraftKings', '{{ source_url }}/dfs/draftkings/cup/race-fantasy-points/')
ON DUPLICATE KEY UPDATE
    url = VALUES(url),
    deleted_at = NULL
;

---

DELETE dsal FROM nascar.dfs_salaries dsal
INNER JOIN nascar.races r ON r.id = dsal.race_id AND r.week_id = {{ week_id }} AND r.deleted_at IS NULL
INNER JOIN nascar.dfs d ON d.id = dsal.dfs_id AND d.name = 'DraftKings' AND d.deleted_at IS NULL;

---

INSERT INTO nascar.dfs_salaries (dfs_id, race_id, dfs_driver_id, car_driver_id, salary)
SELECT
    ranked.dfs_id,
    ranked.race_id,
    ranked.dfs_driver_id,
    ranked.car_driver_id,
    ranked.salary
FROM (
    SELECT
        d.id AS dfs_id,
        r.id AS race_id,
        CAST(stg.rank AS UNSIGNED) AS dfs_driver_id,
        COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) AS car_driver_id,
        CAST(stg.salary AS UNSIGNED) AS salary,
        ROW_NUMBER() OVER (
            PARTITION BY d.id, r.id, COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id)
            ORDER BY CAST(stg.salary AS UNSIGNED) DESC, CAST(stg.rank AS UNSIGNED)
        ) AS rn
    FROM nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    JOIN nascar.dfs d
        ON d.name = 'DraftKings'
        AND d.deleted_at IS NULL
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_num
        ON stg.number = tc_num.car_number
    LEFT JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc_driver
        ON tc_num.car_driver_id IS NULL
        AND stg.driver_name = tc_driver.driver_name
    WHERE COALESCE(tc_num.car_driver_id, tc_driver.car_driver_id) IS NOT NULL
        AND CAST(COALESCE(stg.salary, '0') AS UNSIGNED) > 0
) AS ranked
WHERE ranked.rn = 1
;
