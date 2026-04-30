DROP TABLE IF EXISTS nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }} (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ draftkings_csv_file_path }}'
INTO TABLE nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }}
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
    rank = nascar.clean_unsigned_integer(@rank),
    salary = nascar.clean_unsigned_integer(@salary),
    driver_name = nascar.clean_driver_name(@driver_name),
    team = nascar.clean_team_name(@team),
    number = nascar.clean_car_number(@number),
    manufacturer = nascar.clean_manufacturer_name(@manufacturer),
    laps = nascar.clean_unsigned_integer(@laps),
    driver_rating = nascar.clean_unsigned_decimal(@driver_rating),
    start = nascar.clean_unsigned_integer(@start),
    finish = nascar.clean_unsigned_integer(@finish),
    finish_points = nascar.clean_decimal(@finish_points),
    place_differential_points = nascar.clean_decimal(@place_differential_points),
    fastest_laps = nascar.clean_unsigned_integer(@fastest_laps),
    pct_fastest_laps = nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_fastest_laps, '%', ''), ',', '')),
    fastest_laps_points = nascar.clean_decimal(@fastest_laps_points),
    laps_led = nascar.clean_unsigned_integer(@laps_led),
    pct_laps_led = nascar.clean_unsigned_decimal(REPLACE(REPLACE(@pct_laps_led, '%', ''), ',', '')),
    laps_led_points = nascar.clean_decimal(@laps_led_points),
    total_points = nascar.clean_decimal(@total_points)
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
    number,
    driver_name,
    manufacturer,
    team,
    ds.id
FROM nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

---

INSERT INTO nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} (week_id, car_number, driver_name, manufacturer_name, team_name, data_source_id)
SELECT
    {{ week_id }},
    number,
    driver_name,
    manufacturer,
    team,
    ds.id
FROM nascar.stg_frcs_draftkings_{{ week_id }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

