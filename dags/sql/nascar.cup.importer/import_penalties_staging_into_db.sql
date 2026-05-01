DELETE pen FROM nascar.penalties pen
INNER JOIN nascar.races r ON r.id = pen.race_id
WHERE r.week_id = {{ week_id }};

---

INSERT INTO nascar.penalties (race_id, car_driver_id, lap, flag, description, penalty)
SELECT
    ranked.race_id,
    ranked.car_driver_id,
    ranked.lap,
    ranked.flag,
    ranked.description,
    ranked.penalty
FROM (
    SELECT
        r.id AS race_id,
        tc.car_driver_id,
        CAST(stg.lap AS UNSIGNED) AS lap,
        stg.flag AS flag,
        stg.infraction AS description,
        stg.penalty AS penalty,
        ROW_NUMBER() OVER (
            PARTITION BY
                r.id,
                tc.car_driver_id,
                CAST(stg.lap AS UNSIGNED)
            ORDER BY stg.infraction, stg.penalty
        ) AS rn
    FROM nascar.stg_frcs_penalties_{{ week_id }}_{{ table_suffix }} stg
    JOIN nascar.races r
        ON r.week_id = {{ week_id }}
        AND r.deleted_at IS NULL
    JOIN nascar.tmp_car_{{ week_id }}_{{ table_suffix }} tc
        ON stg.driver_name = tc.driver_name
    WHERE stg.penalty IS NOT NULL
        AND stg.lap IS NOT NULL
        AND tc.car_driver_id IS NOT NULL
) ranked
WHERE ranked.rn = 1
;
