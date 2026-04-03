DROP TABLE IF EXISTS nascar.stg_frcs_lineup_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_lineup_{{ week_id }}_{{ table_suffix }} (
    start VARCHAR(255) NULL,
    number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    sponsor VARCHAR(255) NULL,
    team VARCHAR(255) NULL,
    crew_chief VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ lineup_csv_file_path }}'
INTO TABLE nascar.stg_frcs_lineup_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @start,
    @number,
    @driver_name,
    @manufacturer,
    @sponsor,
    @team,
    @crew_chief
)
SET
    start = NULLIF(TRIM(@start), ''),
    number = NULLIF(TRIM(@number), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    sponsor = NULLIF(TRIM(@sponsor), ''),
    team = NULLIF(TRIM(@team), ''),
    crew_chief = NULLIF(TRIM(@crew_chief), '')
;
