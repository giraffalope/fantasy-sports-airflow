INSERT INTO nascar.pitstops (
    race_id,
    car_driver_id,
    leader_lap,
    driver_lap,
    type,
    stop_time,
    driver_time
)
SELECT
    r.id AS race_id,
    cd.id AS car_driver_id,
    CAST(stg.leader_lap AS UNSIGNED) AS leader_lap,
    CAST(stg.pitter_lap AS UNSIGNED) AS driver_lap,
    CASE
        WHEN UPPER(stg.type) LIKE '%RIGHT%' THEN 'TWO_WHEEL_CHANGE_RIGHT'
        WHEN UPPER(stg.type) LIKE '%LEFT%' THEN 'TWO_WHEEL_CHANGE_LEFT'
        ELSE 'FOUR_WHEEL_CHANGE'
    END AS type,
    CAST(stg.stop_duration AS TIME) AS stop_time,
    CAST(stg.driver_time AS TIME) AS driver_time
FROM nascar.stg_frcs_pit_stops_detail_{{ week_id }} stg
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
WHERE stg.leader_lap IS NOT NULL
    AND stg.pitter_lap IS NOT NULL
    AND cd.id IS NOT NULL
ON DUPLICATE KEY UPDATE
    type = VALUES(type),
    stop_time = VALUES(stop_time),
    driver_time = VALUES(driver_time),
    deleted_at = NULL
;
