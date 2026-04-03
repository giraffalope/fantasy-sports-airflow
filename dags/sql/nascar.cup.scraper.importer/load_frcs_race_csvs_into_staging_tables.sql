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
    position = NULLIF(TRIM(@position), ''),
    starting_position = NULLIF(TRIM(@starting_position), ''),
    number = NULLIF(TRIM(@number), ''),
    name = NULLIF(TRIM(@name), ''),
    sponsor = NULLIF(TRIM(@sponsor), ''),
    owner = NULLIF(TRIM(@owner), ''),
    car = NULLIF(TRIM(@car), ''),
    laps = NULLIF(TRIM(@laps), ''),
    status = NULLIF(TRIM(@status), ''),
    laps_led = NULLIF(TRIM(@laps_led), ''),
    points = NULLIF(TRIM(@points), ''),
    playoff_points = NULLIF(TRIM(@playoff_points), '')
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
    driver_name = NULLIF(TRIM(@driver_name), ''),
    start = NULLIF(TRIM(@start), ''),
    mid_race = NULLIF(TRIM(@mid_race), ''),
    finish = NULLIF(TRIM(@finish), ''),
    high_pos = NULLIF(TRIM(@high_pos), ''),
    low_pos = NULLIF(TRIM(@low_pos), ''),
    avg_pos = NULLIF(TRIM(@avg_pos), ''),
    pass_diff = NULLIF(TRIM(@pass_diff), ''),
    green_flag_passes = NULLIF(TRIM(@green_flag_passes), ''),
    green_flag_times_passed = NULLIF(TRIM(@green_flag_times_passed), ''),
    quality_passes = NULLIF(TRIM(@quality_passes), ''),
    pct_quality_passes = NULLIF(TRIM(@pct_quality_passes), ''),
    fastest_lap = NULLIF(TRIM(@fastest_lap), ''),
    pct_top_15_laps = NULLIF(TRIM(@pct_top_15_laps), ''),
    laps_led = NULLIF(TRIM(@laps_led), ''),
    pct_laps_led = NULLIF(TRIM(@pct_laps_led), ''),
    total_laps = NULLIF(TRIM(@total_laps), ''),
    driver_rating = NULLIF(TRIM(@driver_rating), ''),
    top_15_laps = CASE
        WHEN NULLIF(TRIM(@total_laps), '') REGEXP '^[0-9]+$'
            AND NULLIF(
                REPLACE(REPLACE(TRIM(@pct_top_15_laps), '%', ''), ',', ''),
                ''
            ) REGEXP '^[0-9]+(\\.[0-9]*)?$'
        THEN CAST(
            ROUND(
                CAST(TRIM(@total_laps) AS DECIMAL(20, 6)) * CAST(
                    REPLACE(REPLACE(TRIM(@pct_top_15_laps), '%', ''), ',', '')
                    AS DECIMAL(20, 6)
                ) / 100.0,
                0
            ) AS CHAR
        )
        ELSE NULL
    END
;
