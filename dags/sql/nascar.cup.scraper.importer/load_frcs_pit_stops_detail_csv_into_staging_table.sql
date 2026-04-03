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
    number = NULLIF(TRIM(@number), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    leader_lap = NULLIF(TRIM(@leader_lap), ''),
    pitter_lap = NULLIF(TRIM(@pitter_lap), ''),
    type = NULLIF(TRIM(@type), ''),
    stop_duration = NULLIF(TRIM(@stop_duration), ''),
    driver_time = NULLIF(TRIM(@driver_time), ''),
    total_duration = NULLIF(TRIM(@total_duration), '')
;
