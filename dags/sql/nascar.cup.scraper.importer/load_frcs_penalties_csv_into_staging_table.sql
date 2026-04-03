DROP TABLE IF EXISTS nascar.stg_frcs_penalties_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.stg_frcs_penalties_{{ week_id }}_{{ table_suffix }} (
    driver_name VARCHAR(255) NULL,
    lap VARCHAR(255) NULL,
    flag VARCHAR(255) NULL,
    infraction VARCHAR(255) NULL,
    penalty VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ penalties_csv_file_path }}'
INTO TABLE nascar.stg_frcs_penalties_{{ week_id }}_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @driver_name,
    @lap,
    @flag,
    @infraction,
    @penalty
)
SET
    driver_name = NULLIF(TRIM(@driver_name), ''),
    lap = NULLIF(TRIM(@lap), ''),
    flag = NULLIF(TRIM(@flag), ''),
    infraction = NULLIF(TRIM(@infraction), ''),
    penalty = NULLIF(TRIM(@penalty), '')
;
