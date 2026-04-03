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
    rank = NULLIF(TRIM(@rank), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    number = NULLIF(TRIM(@number), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    time = NULLIF(TRIM(@time), ''),
    speed = NULLIF(TRIM(@speed), '')
;
