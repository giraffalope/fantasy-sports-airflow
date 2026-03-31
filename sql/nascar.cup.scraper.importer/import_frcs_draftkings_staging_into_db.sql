INSERT INTO nascar.dfs (name, url)
VALUES ('DraftKings', '{{ source_url }}/dfs/draftkings/cup/race-fantasy-points/')
ON DUPLICATE KEY UPDATE
    url = VALUES(url),
    deleted_at = NULL
;

---

INSERT INTO nascar.data_sources_drivers (data_source_id, full_name)
SELECT DISTINCT ds.id, TRIM(stg.driver_name)
FROM nascar.stg_frcs_draftkings_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE stg.driver_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.dfs_salaries (dfs_id, race_id, dfs_driver_id, car_driver_id, salary)
SELECT
    d.id AS dfs_id,
    r.id AS race_id,
    CAST(stg.rank AS UNSIGNED) AS dfs_driver_id,
    cd.id AS car_driver_id,
    CAST(stg.salary AS UNSIGNED) AS salary
FROM nascar.stg_frcs_draftkings_{{ week_id }} stg
JOIN nascar.races r
    ON r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
JOIN nascar.dfs d
    ON d.name = 'DraftKings'
    AND d.deleted_at IS NULL
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
LEFT JOIN nascar.data_sources_drivers dsd
    ON dsd.data_source_id = ds.id
    AND dsd.deleted_at IS NULL
    AND TRIM(dsd.full_name) = TRIM(stg.driver_name)
LEFT JOIN nascar.cars c
    ON c.number = TRIM(stg.number)
    AND c.deleted_at IS NULL
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = c.id
    AND (cd.driver_id = dsd.driver_id OR dsd.driver_id IS NULL)
    AND cd.deleted_at IS NULL
WHERE stg.salary IS NOT NULL
    AND cd.id IS NOT NULL
ON DUPLICATE KEY UPDATE
    dfs_driver_id = VALUES(dfs_driver_id),
    salary = VALUES(salary),
    deleted_at = NULL
;
