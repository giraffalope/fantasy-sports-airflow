DROP TABLE IF EXISTS nascar.stg_frcs_practice_{{ week_id }}_{{ number }};

---

CREATE TABLE nascar.stg_frcs_practice_{{ week_id }}_{{ number }} (
    rank VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    time VARCHAR(255) NULL,
    speed VARCHAR(255) NULL,
    num_laps VARCHAR(255) NULL,
    best_lap VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ practice_csv_file_path }}'
INTO TABLE nascar.stg_frcs_practice_{{ week_id }}_{{ number }}
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
    best_lap = NULLIF(TRIM(@best_lap), '')
;
