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
    stop_time,
    driver_time
)
SELECT
    ranked.race_id,
    ranked.car_driver_id,
    ranked.leader_lap,
    ranked.driver_lap,
    ranked.type,
    ranked.stop_time,
    ranked.driver_time
FROM (
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
        COALESCE(
            CASE
                WHEN TRIM(stg.stop_duration) LIKE '%:%' THEN CAST(TRIM(stg.stop_duration) AS TIME(3))
                WHEN TRIM(stg.stop_duration) REGEXP '^[0-9]+(\\.[0-9]*)?$' THEN CAST(
                    SEC_TO_TIME(CAST(TRIM(stg.stop_duration) AS DECIMAL(12, 3))) AS TIME(3)
                )
                ELSE NULL
            END,
            CAST('00:00:00' AS TIME(3))
        ) AS stop_time,
        COALESCE(
            CASE
                WHEN TRIM(stg.driver_time) LIKE '%:%' THEN CAST(TRIM(stg.driver_time) AS TIME(3))
                WHEN TRIM(stg.driver_time) REGEXP '^[0-9]+(\\.[0-9]*)?$' THEN CAST(
                    SEC_TO_TIME(CAST(TRIM(stg.driver_time) AS DECIMAL(12, 3))) AS TIME(3)
                )
                ELSE NULL
            END,
            CAST('00:00:00' AS TIME(3))
        ) AS driver_time,
        ROW_NUMBER() OVER (
            PARTITION BY r.id, cd.id, CAST(stg.leader_lap AS UNSIGNED), CAST(stg.pitter_lap AS UNSIGNED)
            ORDER BY COALESCE(TRIM(stg.stop_duration), ''), COALESCE(TRIM(stg.driver_name), ''), COALESCE(TRIM(stg.number), '')
        ) AS rn
    FROM nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    JOIN nascar.data_sources ds
        ON ds.name = '{{ source_name }}'
        AND ds.deleted_at IS NULL
    LEFT JOIN nascar.data_sources_drivers dsd
        ON dsd.data_source_id = ds.id
        AND dsd.deleted_at IS NULL
        AND TRIM(dsd.name) = TRIM(stg.driver_name)
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
) AS ranked
WHERE ranked.rn = 1
;
