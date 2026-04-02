DELETE pen FROM nascar.penalties pen
INNER JOIN nascar.races r ON r.id = pen.race_id
WHERE r.week_id = {{ week_id }};

---

INSERT INTO nascar.data_sources_drivers (data_source_id, name)
SELECT DISTINCT ds.id, TRIM(stg.driver_name)
FROM nascar.stg_frcs_penalties_{{ week_id }} stg
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE stg.driver_name IS NOT NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL;

---

INSERT INTO nascar.drivers (first_name, last_name, suffix)
SELECT x.first_name, x.last_name, x.suffix
FROM (
    SELECT DISTINCT
        nascar.get_driver_first_name(dsd.name) AS first_name,
        nascar.get_driver_last_name(dsd.name) AS last_name,
        nascar.get_driver_suffix(dsd.name) AS suffix
    FROM nascar.data_sources_drivers dsd
    JOIN nascar.data_sources ds
        ON ds.id = dsd.data_source_id
        AND ds.name = '{{ source_name }}'
        AND ds.deleted_at IS NULL
    WHERE dsd.driver_id IS NULL
) x
WHERE NOT EXISTS (
    SELECT 1
    FROM nascar.drivers d
    WHERE d.deleted_at IS NULL
        AND (d.first_name COLLATE utf8mb4_unicode_ci) <=> (x.first_name COLLATE utf8mb4_unicode_ci)
        AND (d.last_name COLLATE utf8mb4_unicode_ci) <=> (x.last_name COLLATE utf8mb4_unicode_ci)
        AND (d.suffix COLLATE utf8mb4_unicode_ci) <=> (x.suffix COLLATE utf8mb4_unicode_ci)
);

---

UPDATE nascar.data_sources_drivers dsd
JOIN nascar.data_sources ds
    ON ds.id = dsd.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.drivers d
    ON (d.first_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_first_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.last_name COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_last_name(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND (d.suffix COLLATE utf8mb4_unicode_ci) = (nascar.get_driver_suffix(dsd.name) COLLATE utf8mb4_unicode_ci)
    AND d.deleted_at IS NULL
SET dsd.driver_id = d.id
WHERE dsd.driver_id IS NULL;

---

INSERT INTO nascar.penalties (race_id, car_driver_id, lap, flag, description, penalty)
SELECT
    race_id,
    car_driver_id,
    lap,
    flag,
    description,
    penalty
FROM (
    SELECT
        r.id AS race_id,
        cd_by_driver.id AS car_driver_id,
        CAST(stg.lap AS UNSIGNED) AS lap,
        CASE
            WHEN UPPER(stg.flag) LIKE 'GREEN%' THEN 'GREEN'
            WHEN UPPER(stg.flag) LIKE 'YELLOW%' THEN 'YELLOW'
            WHEN UPPER(stg.flag) LIKE 'WHITE%' THEN 'WHITE'
            WHEN UPPER(stg.flag) LIKE 'CHECKERED%' THEN 'CHECKERED'
            ELSE 'GREEN'
        END AS flag,
        stg.infraction AS description,
        stg.penalty AS penalty,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.id,
                cd_by_driver.id,
                CAST(stg.lap AS UNSIGNED)
            ORDER BY stg.infraction, stg.penalty
        ) AS rn
    FROM nascar.stg_frcs_penalties_{{ week_id }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    JOIN nascar.data_sources ds
        ON ds.name = '{{ source_name }}'
        AND ds.deleted_at IS NULL
    LEFT JOIN (
        SELECT
            dsd1.id,
            dsd1.data_source_id,
            TRIM(dsd1.name) AS name_trim
        FROM nascar.data_sources_drivers dsd1
        INNER JOIN (
            SELECT data_source_id, TRIM(name) AS name_trim, MIN(id) AS min_id
            FROM nascar.data_sources_drivers
            WHERE deleted_at IS NULL
            GROUP BY data_source_id, TRIM(name)
        ) pick
            ON pick.data_source_id = dsd1.data_source_id
            AND pick.name_trim = TRIM(dsd1.name)
            AND pick.min_id = dsd1.id
        WHERE dsd1.deleted_at IS NULL
    ) dsd_pick
        ON dsd_pick.data_source_id = ds.id
        AND dsd_pick.name_trim = TRIM(stg.driver_name)
    LEFT JOIN nascar.data_sources_drivers dsd
        ON dsd.id = dsd_pick.id
    LEFT JOIN (
        SELECT driver_id, MIN(id) AS id
        FROM nascar.cars_drivers
        WHERE deleted_at IS NULL
        GROUP BY driver_id
    ) cd_by_driver
        ON cd_by_driver.driver_id = dsd.driver_id
    WHERE stg.penalty IS NOT NULL
        AND stg.lap IS NOT NULL
        AND cd_by_driver.id IS NOT NULL
) ranked
WHERE ranked.rn = 1
;
