DROP TABLE IF EXISTS nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }} (
    rank VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    time VARCHAR(255) NULL,
    speed VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ qualifying_csv_file_path }}'
INTO TABLE nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }}
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
    @speed
)
SET
    rank = nascar.clean_position(@rank),
    driver_name = nascar.clean_driver_name(@driver_name),
    number = nascar.clean_car_number(@number),
    manufacturer = nascar.clean_manufacturer_name(@manufacturer),
    time = nascar.clean_time(@time),
    speed = nascar.clean_speed(@speed)
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
    NULL,
    ds.id
FROM nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }}
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
FROM nascar.stg_frcs_qualifying_{{ week_id }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

