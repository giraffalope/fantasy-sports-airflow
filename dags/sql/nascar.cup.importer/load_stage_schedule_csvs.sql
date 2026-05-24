DROP TABLE IF EXISTS nascar.stg_cup_frcs_schedule_import_{{ table_suffix }};

---

CREATE TABLE nascar.stg_cup_frcs_schedule_import_{{ table_suffix }} (
    event_number INT UNSIGNED NULL,
    week_number INT UNSIGNED NULL,
    week_url VARCHAR(255) NULL,
    week_date DATE NULL,
    week_track VARCHAR(255) NULL,
    week_track_url VARCHAR(255) NULL,
    week_race_name VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ frcs_schedule_csv_file_path }}'
INTO TABLE nascar.stg_cup_frcs_schedule_import_{{ table_suffix }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @event_number,
    @week_number,
    @week_url,
    @week_date,
    @week_track,
    @week_track_url,
    @week_race_name
)
SET
    event_number = nascar.clean_unsigned_integer(@event_number),
    week_number = nascar.clean_unsigned_integer(@week_number),
    week_url = nascar.clean_string(@week_url),
    week_date = CASE
        WHEN TRIM(@week_date) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@week_date), '%Y-%m-%d')
    END,
    week_track = nascar.clean_string(@week_track),
    week_track_url = nascar.clean_string(@week_track_url),
    week_race_name = nascar.clean_string(@week_race_name)
;

---

DROP TABLE IF EXISTS nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }};

---

CREATE TABLE nascar.stg_cup_racing_reference_schedule_import_{{ table_suffix }} (
    week_number INT UNSIGNED NULL,
    week_url VARCHAR(255) NULL,
    week_date DATE NULL,
    week_track VARCHAR(255) NULL,
    week_track_url VARCHAR(255) NULL,
    week_race_name VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

LOAD DATA LOCAL INFILE '{{ racing_reference_schedule_csv_file_path }}'
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
    week_number = nascar.clean_unsigned_integer(@week_number),
    week_url = nascar.clean_string(@week_url),
    week_date = CASE
        WHEN TRIM(@week_date) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@week_date), '%Y-%m-%d')
    END,
    week_track = nascar.clean_string(@week_track),
    week_track_url = nascar.clean_string(@week_track_url),
    week_race_name = nascar.clean_string(@week_race_name)
;