INSERT INTO nascar.penalties (race_id, car_driver_id, lap, flag, description, penalty)
SELECT
    r.id AS race_id,
    cd.id AS car_driver_id,
    CAST(stg.lap AS UNSIGNED) AS lap,
    CASE
        WHEN UPPER(stg.flag) LIKE 'GREEN%' THEN 'GREEN'
        WHEN UPPER(stg.flag) LIKE 'YELLOW%' THEN 'YELLOW'
        WHEN UPPER(stg.flag) LIKE 'WHITE%' THEN 'WHITE'
        WHEN UPPER(stg.flag) LIKE 'CHECKERED%' THEN 'CHECKERED'
        ELSE 'GREEN'
    END AS flag,
    stg.infraction AS description,
    stg.penalty AS penalty
FROM nascar.stg_frcs_penalties_{{ week_id }} stg
JOIN nascar.races r
    ON r.week_id = {{ week_id }}
    AND r.deleted_at IS NULL
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
WHERE stg.penalty IS NOT NULL
    AND stg.lap IS NOT NULL
    AND cd.id IS NOT NULL
ON DUPLICATE KEY UPDATE
    flag = VALUES(flag),
    description = VALUES(description),
    penalty = VALUES(penalty),
    deleted_at = NULL
;
