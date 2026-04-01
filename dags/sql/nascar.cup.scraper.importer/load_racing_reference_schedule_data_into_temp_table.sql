DROP TABLE IF EXISTS nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }};

---

CREATE TABLE nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }} (
    week_number INT UNSIGNED NULL,
    week_url VARCHAR(255) NULL,
    week_date DATE NULL,
    week_track VARCHAR(255) NULL,
    week_track_url VARCHAR(255) NULL,
    week_race_name VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ schedule_csv_file_path }}'
INTO TABLE nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @week_number,
    @week_url,
    @week_date,
    @week_track,
    @week_track_url,
    @week_race_name
)
SET
    week_number = CASE
        WHEN TRIM(@week_number) = '' THEN NULL
        ELSE CAST(TRIM(@week_number) AS UNSIGNED)
    END,
    week_url = NULLIF(TRIM(@week_url), ''),
    week_date = CASE
        WHEN TRIM(@week_date) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@week_date), '%Y-%m-%d')
    END,
    week_track = NULLIF(TRIM(@week_track), ''),
    week_track_url = NULLIF(TRIM(@week_track_url), ''),
    week_race_name = NULLIF(TRIM(@week_race_name), '')
;
