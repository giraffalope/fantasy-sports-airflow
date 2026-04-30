DROP TABLE IF EXISTS nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }} (
    position VARCHAR(255) NULL,
    starting_position VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    name VARCHAR(255) NULL,
    sponsor VARCHAR(255) NULL,
    owner VARCHAR(255) NULL,
    car VARCHAR(255) NULL,
    laps VARCHAR(255) NULL,
    status VARCHAR(255) NULL,
    laps_led VARCHAR(255) NULL,
    points VARCHAR(255) NULL,
    playoff_points VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ race_results_csv_file_path }}'
INTO TABLE nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @position,
    @starting_position,
    @number,
    @name,
    @sponsor,
    @owner,
    @car,
    @laps,
    @status,
    @laps_led,
    @points,
    @playoff_points
)
SET
    position = nascar.clean_position(@position),
    starting_position = nascar.clean_position(@starting_position),
    number = nascar.clean_car_number(@number),
    name = nascar.clean_driver_name(@name),
    sponsor = nascar.clean_string(@sponsor),
    owner = nascar.clean_team_name(@owner),
    car = nascar.clean_manufacturer_name(@car),
    laps = nascar.clean_unsigned_integer(@laps),
    status = nascar.clean_car_status(@status),
    laps_led = nascar.clean_unsigned_integer(@laps_led),
    points = nascar.clean_decimal(@points),
    playoff_points = nascar.clean_decimal(@playoff_points)
;

---

DROP TABLE IF EXISTS nascar.stg_frcs_race_loop_data_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_race_loop_data_{{ week_id }}_{{ table_suffix }} (
    driver_name VARCHAR(255) NULL,
    start VARCHAR(255) NULL,
    mid_race VARCHAR(255) NULL,
    finish VARCHAR(255) NULL,
    high_pos VARCHAR(255) NULL,
    low_pos VARCHAR(255) NULL,
    avg_pos VARCHAR(255) NULL,
    pass_diff VARCHAR(255) NULL,
    green_flag_passes VARCHAR(255) NULL,
    green_flag_times_passed VARCHAR(255) NULL,
    quality_passes VARCHAR(255) NULL,
    pct_quality_passes VARCHAR(255) NULL,
    fastest_lap VARCHAR(255) NULL,
    top_15_laps VARCHAR(255) NULL,
    pct_top_15_laps VARCHAR(255) NULL,
    laps_led VARCHAR(255) NULL,
    pct_laps_led VARCHAR(255) NULL,
    total_laps VARCHAR(255) NULL,
    driver_rating VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ race_loop_data_csv_file_path }}'
INTO TABLE nascar.stg_frcs_race_loop_data_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @driver_name,
    @start,
    @mid_race,
    @finish,
    @high_pos,
    @low_pos,
    @avg_pos,
    @pass_diff,
    @green_flag_passes,
    @green_flag_times_passed,
    @quality_passes,
    @pct_quality_passes,
    @fastest_lap,
    @pct_top_15_laps,
    @laps_led,
    @pct_laps_led,
    @total_laps,
    @driver_rating
)
SET
    driver_name = nascar.clean_driver_name(@driver_name),
    start = nascar.clean_position(@start),
    mid_race = nascar.clean_position(@mid_race),
    finish = nascar.clean_position(@finish),
    high_pos = nascar.clean_position(@high_pos),
    low_pos = nascar.clean_position(@low_pos),
    avg_pos = nascar.clean_position(@avg_pos),
    pass_diff = nascar.clean_integer(@pass_diff),
    green_flag_passes = nascar.clean_unsigned_integer(@green_flag_passes),
    green_flag_times_passed = nascar.clean_unsigned_integer(@green_flag_times_passed),
    quality_passes = nascar.clean_unsigned_integer(@quality_passes),
    pct_quality_passes = nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_quality_passes, '%', ''), ',', '')),
    fastest_lap = nascar.clean_unsigned_integer(@fastest_lap),
    pct_top_15_laps = nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_top_15_laps, '%', ''), ',', '')),
    laps_led = nascar.clean_unsigned_integer(@laps_led),
    pct_laps_led = nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_laps_led, '%', ''), ',', '')),
    total_laps = nascar.clean_unsigned_integer(@total_laps),
    driver_rating = nascar.clean_unsigned_decimal(@driver_rating),
    top_15_laps = CASE
        WHEN nascar.clean_unsigned_integer(@total_laps) IS NOT NULL
            AND nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_top_15_laps, '%', ''), ',', '')) IS NOT NULL
        THEN CAST(
            ROUND(
                CAST(nascar.clean_unsigned_integer(@total_laps) AS DECIMAL(20, 6)) * CAST(
                    nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_top_15_laps, '%', ''), ',', ''))
                    AS DECIMAL(20, 6)
                ) / 100.0,
                0
            ) AS CHAR
        )
        ELSE NULL
    END
;

---

CREATE TABLE IF NOT EXISTS nascar.stg_car_{{ week_id }}_{{ table_suffix }} (
    car_number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    data_source_id BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

CREATE TABLE IF NOT EXISTS nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} (
    week_id BIGINT UNSIGNED NOT NULL,
    car_number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    data_source_id BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.stg_car_{{ week_id }}_{{ table_suffix }} (car_number, driver_name, manufacturer_name, team_name, data_source_id)
SELECT
    stg.number,
    stg.name,
    stg.car,
    stg.owner,
    ds.id
FROM nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }} stg
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

---

INSERT INTO nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} (week_id, car_number, driver_name, manufacturer_name, team_name, data_source_id)
SELECT
    {{ week_id }},
    stg.number,
    stg.name,
    stg.car,
    stg.owner,
    ds.id
FROM nascar.stg_frcs_race_results_{{ week_id }}_{{ table_suffix }} stg
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

