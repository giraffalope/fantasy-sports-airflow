DROP TABLE IF EXISTS nascar.stg_frcs_draftkings_{{ week_id }};

---

CREATE TABLE nascar.stg_frcs_draftkings_{{ week_id }} (
    rank VARCHAR(255) NULL,
    salary VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    team VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    laps VARCHAR(255) NULL,
    driver_rating VARCHAR(255) NULL,
    start VARCHAR(255) NULL,
    finish VARCHAR(255) NULL,
    finish_points VARCHAR(255) NULL,
    place_differential_points VARCHAR(255) NULL,
    fastest_laps VARCHAR(255) NULL,
    pct_fastest_laps VARCHAR(255) NULL,
    fastest_laps_points VARCHAR(255) NULL,
    laps_led VARCHAR(255) NULL,
    pct_laps_led VARCHAR(255) NULL,
    laps_led_points VARCHAR(255) NULL,
    total_points VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ draftkings_csv_file_path }}'
INTO TABLE nascar.stg_frcs_draftkings_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @rank,
    @salary,
    @driver_name,
    @team,
    @number,
    @manufacturer,
    @laps,
    @driver_rating,
    @start,
    @finish,
    @finish_points,
    @place_differential_points,
    @fastest_laps,
    @pct_fastest_laps,
    @fastest_laps_points,
    @laps_led,
    @pct_laps_led,
    @laps_led_points,
    @total_points
)
SET
    rank = NULLIF(TRIM(@rank), ''),
    salary = NULLIF(TRIM(@salary), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    team = NULLIF(TRIM(@team), ''),
    number = NULLIF(TRIM(@number), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    laps = NULLIF(TRIM(@laps), ''),
    driver_rating = NULLIF(TRIM(@driver_rating), ''),
    start = NULLIF(TRIM(@start), ''),
    finish = NULLIF(TRIM(@finish), ''),
    finish_points = NULLIF(TRIM(@finish_points), ''),
    place_differential_points = NULLIF(TRIM(@place_differential_points), ''),
    fastest_laps = NULLIF(TRIM(@fastest_laps), ''),
    pct_fastest_laps = NULLIF(TRIM(@pct_fastest_laps), ''),
    fastest_laps_points = NULLIF(TRIM(@fastest_laps_points), ''),
    laps_led = NULLIF(TRIM(@laps_led), ''),
    pct_laps_led = NULLIF(TRIM(@pct_laps_led), ''),
    laps_led_points = NULLIF(TRIM(@laps_led_points), ''),
    total_points = NULLIF(TRIM(@total_points), '')
;
