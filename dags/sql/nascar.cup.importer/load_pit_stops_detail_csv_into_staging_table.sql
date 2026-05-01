DROP TABLE IF EXISTS nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }} (
    number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    leader_lap VARCHAR(255) NULL,
    pitter_lap VARCHAR(255) NULL,
    type VARCHAR(255) NULL,
    stop_duration VARCHAR(255) NULL,
    driver_time VARCHAR(255) NULL,
    total_duration VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ pit_stops_detail_csv_file_path }}'
INTO TABLE nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @number,
    @driver_name,
    @leader_lap,
    @pitter_lap,
    @type,
    @stop_duration,
    @driver_time,
    @total_duration
)
SET
    number = nascar.clean_car_number(@number),
    driver_name = nascar.clean_driver_name(@driver_name),
    leader_lap = nascar.clean_unsigned_integer(@leader_lap),
    pitter_lap = nascar.clean_unsigned_integer(@pitter_lap),
    type = nascar.clean_pitstop_type(@type),
    stop_duration = nascar.clean_time(@stop_duration),
    driver_time = nascar.clean_time(@driver_time),
    total_duration = nascar.clean_time(@total_duration)
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
    NULL,
    NULL,
    ds.id
FROM nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

---

INSERT INTO nascar.stg_car_season_{{ season_id }}_{{ table_suffix }} (week_id, car_number, driver_name, manufacturer_name, team_name, data_source_id)
SELECT
    {{ week_id }},
    number,
    driver_name,
    NULL,
    NULL,
    ds.id
FROM nascar.stg_frcs_pit_stops_detail_{{ week_id }}_{{ table_suffix }}
JOIN nascar.data_sources ds ON ds.name = '{{ frcs_source_name }}' AND ds.deleted_at IS NULL;

