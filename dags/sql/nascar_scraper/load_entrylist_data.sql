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
(number, driver_name, owner, manufacturer, crew_chief, sponsor)
;

---

INSERT INTO nascar.racing_reference_teams (name)
SELECT DISTINCT TRIM(e.owner) AS name
FROM nascar.temp_entrylist_{{ week_id }} e
WHERE TRIM(e.owner) IS NOT NULL AND TRIM(e.owner) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.teams (name)
SELECT DISTINCT TRIM(rrt.name) AS name
FROM nascar.racing_reference_teams rrt
WHERE rrt.team_id IS NULL AND rrt.deleted_at IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.racing_reference_teams rrt
JOIN nascar.teams t
    ON TRIM(t.name) = TRIM(rrt.name)
    AND t.deleted_at IS NULL
SET
    rrt.team_id = t.id
WHERE
    rrt.team_id IS NULL
    AND rrt.deleted_at IS NULL
;

---

UPDATE nascar.temp_entrylist_{{ week_id }} e
LEFT JOIN nascar.teams t
    ON TRIM(t.name) = TRIM(e.owner)
    AND t.deleted_at IS NULL
SET
    e.team_id = t.id
;
