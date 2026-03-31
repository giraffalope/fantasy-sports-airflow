DROP TABLE IF EXISTS nascar.stg_frcs_penalties_{{ week_id }};

---

CREATE TABLE nascar.stg_frcs_penalties_{{ week_id }} (
    number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    lap VARCHAR(255) NULL,
    flag VARCHAR(255) NULL,
    infraction VARCHAR(255) NULL,
    penalty VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ penalties_csv_file_path }}'
INTO TABLE nascar.stg_frcs_penalties_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @number,
    @driver_name,
    @lap,
    @flag,
    @infraction,
    @penalty
)
SET
    number = NULLIF(TRIM(@number), ''),
    driver_name = NULLIF(TRIM(@driver_name), ''),
    lap = NULLIF(TRIM(@lap), ''),
    flag = NULLIF(TRIM(@flag), ''),
    infraction = NULLIF(TRIM(@infraction), ''),
    penalty = NULLIF(TRIM(@penalty), '')
;
