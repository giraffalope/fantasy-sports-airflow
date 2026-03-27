DROP TABLE IF EXISTS nascar.temp_entrylist_{{ week_id }};

---

CREATE TABLE nascar.temp_entrylist_{{ week_id }} (
    number VARCHAR(255),
    driver_name VARCHAR(255),
    owner VARCHAR(255),
    manufacturer VARCHAR(255),
    crew_chief VARCHAR(255),
    sponsor VARCHAR(255),
    team_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

---

LOAD DATA LOCAL INFILE '{{ entrylist_csv_file_path }}'
INTO TABLE nascar.temp_entrylist_{{ week_id }}
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(number, driver_name, owner, manufacturer, crew_chief, sponsor, team_id)
;

---

INSERT IGNORE INTO nascar.teams (rr_name)
SELECT TRIM(e.owner) AS rr_name
FROM nascar.temp_entrylist_{{ week_id }} e
;

---

UPDATE nascar.temp_entrylist_{{ week_id }} e
LEFT JOIN nascar.teams t
    ON t.rr_name = TRIM(e.owner)
SET
    e.team_id = t.id
WHERE
    t.deleted_at IS NULL
;
