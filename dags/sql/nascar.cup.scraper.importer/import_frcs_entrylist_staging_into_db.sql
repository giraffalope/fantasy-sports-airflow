DROP TABLE IF EXISTS nascar.temp_entrylist_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.temp_entrylist_{{ week_id }}_{{ table_suffix }} (
    number VARCHAR(255),
    driver_name VARCHAR(255),
    owner VARCHAR(255),
    manufacturer VARCHAR(255),
    crew_chief VARCHAR(255),
    sponsor VARCHAR(255),
    team_id BIGINT UNSIGNED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.temp_entrylist_{{ week_id }}_{{ table_suffix }} (number, driver_name, owner, manufacturer, crew_chief, sponsor)
SELECT number, driver_name, owner, manufacturer, crew_chief, sponsor
FROM nascar.stg_frcs_entrylist_{{ week_id }}_{{ table_suffix }};

---

INSERT INTO nascar.data_sources_teams (data_source_id, name)
SELECT DISTINCT ds.id AS data_source_id, TRIM(e.owner) AS name
FROM nascar.temp_entrylist_{{ week_id }}_{{ table_suffix }} e
JOIN nascar.data_sources ds
    ON ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE TRIM(e.owner) IS NOT NULL AND TRIM(e.owner) != ''
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

INSERT INTO nascar.teams (series_id, name)
SELECT DISTINCT {{ series_id }}, TRIM(dst.name) AS name
FROM nascar.data_sources_teams dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
WHERE dst.team_id IS NULL AND dst.deleted_at IS NULL
ON DUPLICATE KEY UPDATE deleted_at = NULL
;

---

UPDATE nascar.data_sources_teams dst
JOIN nascar.data_sources ds
    ON ds.id = dst.data_source_id
    AND ds.name = '{{ source_name }}'
    AND ds.deleted_at IS NULL
JOIN nascar.teams t
    ON TRIM(t.name) = TRIM(dst.name)
    AND t.deleted_at IS NULL
SET dst.team_id = t.id
WHERE dst.team_id IS NULL AND dst.deleted_at IS NULL
;

---

UPDATE nascar.temp_entrylist_{{ week_id }}_{{ table_suffix }} e
LEFT JOIN nascar.teams t
    ON TRIM(t.name) = TRIM(e.owner)
    AND t.deleted_at IS NULL
SET e.team_id = t.id
;
