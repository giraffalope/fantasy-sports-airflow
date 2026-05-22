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
    rank = nascar.clean_position(@rank),
    driver_name = nascar.clean_driver_name(@driver_name),
    number = nascar.clean_car_number(@number),
    manufacturer = nascar.clean_manufacturer_name(@manufacturer),
    time = nascar.clean_time(@time),
    speed = nascar.clean_speed(@speed),
    num_laps = if(nascar.clean_unsigned_integer(@num_laps) > 0, nascar.clean_unsigned_integer(@num_laps), NULL),
    best_lap = if(nascar.clean_unsigned_integer(@best_lap) > 0, nascar.clean_unsigned_integer(@best_lap), NULL),
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
    rank = nascar.clean_position(@rank),
    driver_name = nascar.clean_driver_name(@driver_name),
    number = nascar.clean_car_number(@number),
    manufacturer = nascar.clean_manufacturer_name(@manufacturer),
    time = nascar.clean_time(@time),
    speed = nascar.clean_speed(@speed),
    num_laps = if(nascar.clean_unsigned_integer(@num_laps) > 0, nascar.clean_unsigned_integer(@num_laps), NULL),
    best_lap = if(nascar.clean_unsigned_integer(@best_lap) > 0, nascar.clean_unsigned_integer(@best_lap), NULL),
    `5_lap_average_time` = nascar.clean_time(@five_lap_avg),
    `10_lap_average_time` = nascar.clean_time(@ten_lap_avg),
    `20_lap_average_time` = nascar.clean_time(@twenty_lap_avg),
    `25_lap_average_time` = nascar.clean_time(@twentyfive_lap_avg),
    `30_lap_average_time` = nascar.clean_time(@thirty_lap_avg),
    `10_lap_average_speed` = nascar.clean_speed(@ten_lap_avg_speed),
    `10_lap_start` = if(nascar.clean_unsigned_integer(@ten_lap_start) > 0, nascar.clean_unsigned_integer(@ten_lap_start), NULL),
    `10_lap_end` = if(nascar.clean_unsigned_integer(@ten_lap_end) > 0, nascar.clean_unsigned_integer(@ten_lap_end), NULL)
;
{% endif %}

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
    NULL,
    ds.id
FROM nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

---

INSERT INTO nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} (week_id, car_number, driver_name, manufacturer_name, team_name, data_source_id)
SELECT
    {{ week_id }},
    number,
    driver_name,
    manufacturer,
    NULL,
    ds.id
FROM nascar.stg_frcs_practice_{{ week_id }}_{{ number }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

