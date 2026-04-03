DROP TABLE IF EXISTS nascar.stg_frcs_entrylist_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_entrylist_{{ week_id }}_{{ table_suffix }} (
    number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    owner VARCHAR(255) NULL,
    manufacturer VARCHAR(255) NULL,
    crew_chief VARCHAR(255) NULL,
    sponsor VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ entrylist_csv_file_path }}'
INTO TABLE nascar.stg_frcs_entrylist_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @number,
    @driver_name,
    @owner,
    @manufacturer,
    @crew_chief,
    @sponsor
)
SET
    number = NULLIF(TRIM(@number), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    owner = NULLIF(TRIM(@owner), ''),
    manufacturer = NULLIF(TRIM(@manufacturer), ''),
    crew_chief = NULLIF(TRIM(@crew_chief), ''),
    sponsor = NULLIF(TRIM(@sponsor), '')
;
