DROP TABLE IF EXISTS nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }} (
    rank VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    time VARCHAR(255) NULL,
    speed VARCHAR(255) NULL,
    num_laps VARCHAR(255) NULL,
    best_lap VARCHAR(255) NULL,
    `5_lap_average_time` VARCHAR(255) NULL,
    `10_lap_average_time` VARCHAR(255) NULL,
    `20_lap_average_time` VARCHAR(255) NULL,
    `25_lap_average_time` VARCHAR(255) NULL,
    `30_lap_average_time` VARCHAR(255) NULL,
    `10_lap_average_speed` VARCHAR(255) NULL,
    `10_lap_start` VARCHAR(255) NULL,
    `10_lap_end` VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

{# FRCS CSV: 16 value columns. Racing Reference: 8 (no multi-lap averages / 10-lap window). #}
{% if practice_csv_source == 'racing_reference' %}
LOAD DATA LOCAL INFILE '{{ practice_csv_file_path }}'
INTO TABLE nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @rank,
    @driver_name,
    @number,
    @manufacturer,
    @time,
    @speed,
    @num_laps,
    @best_lap
)
SET
    rank = NULLIF(TRIM(@rank), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    number = NULLIF(TRIM(@number), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    time = NULLIF(TRIM(@time), ''),
    speed = NULLIF(TRIM(@speed), ''),
    num_laps = NULLIF(TRIM(@num_laps), ''),
    best_lap = NULLIF(TRIM(@best_lap), ''),
    `5_lap_average_time` = NULL,
    `10_lap_average_time` = NULL,
    `20_lap_average_time` = NULL,
    `25_lap_average_time` = NULL,
    `30_lap_average_time` = NULL,
    `10_lap_average_speed` = NULL,
    `10_lap_start` = NULL,
    `10_lap_end` = NULL
;
{% else %}
LOAD DATA LOCAL INFILE '{{ practice_csv_file_path }}'
INTO TABLE nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @rank,
    @driver_name,
    @number,
    @manufacturer,
    @time,
    @speed,
    @num_laps,
    @best_lap,
    @five_lap_avg,
    @ten_lap_avg,
    @twenty_lap_avg,
    @twentyfive_lap_avg,
    @thirty_lap_avg,
    @ten_lap_avg_speed,
    @ten_lap_start,
    @ten_lap_end
)
SET
    rank = NULLIF(TRIM(@rank), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    number = NULLIF(TRIM(@number), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    time = NULLIF(TRIM(@time), ''),
    speed = NULLIF(TRIM(@speed), ''),
    num_laps = NULLIF(TRIM(@num_laps), ''),
    best_lap = NULLIF(TRIM(@best_lap), ''),
    `5_lap_average_time` = NULLIF(TRIM(@five_lap_avg), ''),
    `10_lap_average_time` = NULLIF(TRIM(@ten_lap_avg), ''),
    `20_lap_average_time` = NULLIF(TRIM(@twenty_lap_avg), ''),
    `25_lap_average_time` = NULLIF(TRIM(@twentyfive_lap_avg), ''),
    `30_lap_average_time` = NULLIF(TRIM(@thirty_lap_avg), ''),
    `10_lap_average_speed` = NULLIF(TRIM(@ten_lap_avg_speed), ''),
    `10_lap_start` = NULLIF(TRIM(@ten_lap_start), ''),
    `10_lap_end` = NULLIF(TRIM(@ten_lap_end), '')
;
{% endif %}
