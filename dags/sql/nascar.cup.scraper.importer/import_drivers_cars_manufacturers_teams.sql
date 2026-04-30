DROP TABLE IF EXISTS nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} (
    week_id BIGINT UNSIGNED NOT NULL,
    car_number VARCHAR(255) NULL,
    rr_car_number VARCHAR(255) NULL,
    frcs_car_number VARCHAR(255) NULL,
    rr_driver_name VARCHAR(255) NULL,
    frcs_driver_name VARCHAR(255) NULL,
    rr_manufacturer_name VARCHAR(255) NULL,
    frcs_manufacturer_name VARCHAR(255) NULL,
    rr_team_name VARCHAR(255) NULL,
    frcs_team_name VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    driver_id BIGINT UNSIGNED NULL,
    manufacturer_id BIGINT UNSIGNED NULL,
    team_id BIGINT UNSIGNED NULL,
    car_id BIGINT UNSIGNED NULL,
    car_driver_id BIGINT UNSIGNED NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

CREATE TABLE IF NOT EXISTS nascar.tmp_preload_driver_car_wide_{{ table_suffix }} (
    week_id BIGINT UNSIGNED NOT NULL,
    car_number VARCHAR(255) NULL,
    driver_name VARCHAR(255) NULL,
    manufacturer_name VARCHAR(255) NULL,
    team_name VARCHAR(255) NULL,
    driver_id BIGINT UNSIGNED NULL,
    manufacturer_id BIGINT UNSIGNED NULL,
    team_id BIGINT UNSIGNED NULL,
    car_id BIGINT UNSIGNED NULL,
    car_driver_id BIGINT UNSIGNED NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

---

INSERT INTO nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} (
    week_id,
    car_number,
    rr_car_number,
    frcs_car_number,
    frcs_driver_name,
    frcs_manufacturer_name,
    frcs_team_name
)
SELECT DISTINCT
    {{ week_id }} AS week_id,
    nascar.clean_car_number(stg.number) AS car_number,
    NULL AS rr_car_number,
    nascar.clean_car_number(stg.number) AS frcs_car_number,
    nascar.clean_driver_name(stg.driver_name) AS frcs_driver_name,
    nascar.clean_manufacturer_name(stg.manufacturer_name) AS frcs_manufacturer_name,
    nascar.clean_team_name(stg.team_name) AS frcs_team_name
FROM nascar.stg_frcs_lineup_drivers_cars_{{ week_id }}_{{ table_suffix }} stg
WHERE NULLIF(TRIM(stg.driver_name), '') IS NOT NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.stg_rr_drivers_cars_{{ week_id }}_{{ table_suffix }} rr
    ON nascar.clean_driver_name(rr.driver_name) = t.frcs_driver_name
SET
    t.rr_car_number = nascar.clean_car_number(rr.number),
    t.rr_driver_name = nascar.clean_driver_name(rr.driver_name),
    t.rr_manufacturer_name = nascar.clean_manufacturer_name(rr.manufacturer_name)
WHERE rr.driver_name IS NOT NULL;

---

INSERT INTO nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} (
    week_id,
    car_number,
    rr_car_number,
    rr_driver_name,
    rr_manufacturer_name
)
SELECT DISTINCT
    {{ week_id }} AS week_id,
    nascar.clean_car_number(rr.number) AS car_number,
    nascar.clean_car_number(rr.number) AS rr_car_number,
    nascar.clean_driver_name(rr.driver_name) AS rr_driver_name,
    nascar.clean_manufacturer_name(rr.manufacturer_name) AS rr_manufacturer_name
FROM nascar.stg_rr_drivers_cars_{{ week_id }}_{{ table_suffix }} rr
WHERE nascar.clean_driver_name(rr.driver_name) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
      WHERE COALESCE(t.frcs_driver_name, t.rr_driver_name, '') = nascar.clean_driver_name(rr.driver_name)
  );

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
SET
    t.driver_name = COALESCE(t.frcs_driver_name, t.rr_driver_name),
    t.car_number = COALESCE(t.rr_car_number, t.frcs_car_number),
    t.team_name = COALESCE(t.frcs_team_name, t.rr_team_name),
    t.manufacturer_name = COALESCE(t.frcs_manufacturer_name, t.rr_manufacturer_name);

---

INSERT INTO nascar.tmp_preload_driver_car_wide_{{ table_suffix }} (
    week_id,
    car_number,
    driver_name,
    manufacturer_name,
    team_name
)
SELECT
    t.week_id,
    t.car_number,
    t.driver_name,
    t.manufacturer_name,
    t.team_name
FROM nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
WHERE t.driver_name IS NOT NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.data_sources ds_frcs
    ON ds_frcs.name = '{{ frcs_source_name }}'
   AND ds_frcs.deleted_at IS NULL
LEFT JOIN nascar.data_sources_drivers dsd_frcs
    ON dsd_frcs.data_source_id = ds_frcs.id
   AND dsd_frcs.deleted_at IS NULL
   AND nascar.clean_driver_name(dsd_frcs.name) = t.frcs_driver_name
LEFT JOIN nascar.data_sources ds_rr
    ON ds_rr.name = '{{ racing_reference_source_name }}'
   AND ds_rr.deleted_at IS NULL
LEFT JOIN nascar.data_sources_drivers dsd_rr
    ON dsd_rr.data_source_id = ds_rr.id
   AND dsd_rr.deleted_at IS NULL
   AND nascar.clean_driver_name(dsd_rr.name) = t.rr_driver_name
SET t.driver_id = COALESCE(dsd_frcs.driver_id, dsd_rr.driver_id)
WHERE t.driver_id IS NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.data_sources ds_frcs
    ON ds_frcs.name = '{{ frcs_source_name }}'
   AND ds_frcs.deleted_at IS NULL
LEFT JOIN nascar.data_sources_manufacturers dsm_frcs
    ON dsm_frcs.data_source_id = ds_frcs.id
   AND dsm_frcs.deleted_at IS NULL
   AND nascar.clean_manufacturer_name(dsm_frcs.name) = t.frcs_manufacturer_name
LEFT JOIN nascar.data_sources ds_rr
    ON ds_rr.name = '{{ racing_reference_source_name }}'
   AND ds_rr.deleted_at IS NULL
LEFT JOIN nascar.data_sources_manufacturers dsm_rr
    ON dsm_rr.data_source_id = ds_rr.id
   AND dsm_rr.deleted_at IS NULL
   AND nascar.clean_manufacturer_name(dsm_rr.name) = t.rr_manufacturer_name
SET t.manufacturer_id = COALESCE(dsm_frcs.manufacturer_id, dsm_rr.manufacturer_id)
WHERE t.manufacturer_id IS NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.data_sources ds_frcs
    ON ds_frcs.name = '{{ frcs_source_name }}'
   AND ds_frcs.deleted_at IS NULL
LEFT JOIN nascar.data_sources_teams dst_frcs
    ON dst_frcs.data_source_id = ds_frcs.id
   AND dst_frcs.deleted_at IS NULL
   AND nascar.clean_team_name(dst_frcs.name) = t.frcs_team_name
LEFT JOIN nascar.data_sources ds_rr
    ON ds_rr.name = '{{ racing_reference_source_name }}'
   AND ds_rr.deleted_at IS NULL
LEFT JOIN nascar.data_sources_teams dst_rr
    ON dst_rr.data_source_id = ds_rr.id
   AND dst_rr.deleted_at IS NULL
   AND nascar.clean_team_name(dst_rr.name) = t.rr_team_name
SET t.team_id = COALESCE(dst_frcs.team_id, dst_rr.team_id)
WHERE t.team_id IS NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.cars c
    ON c.team_id = t.team_id
   AND c.manufacturer_id = t.manufacturer_id
   AND c.number = t.car_number
   AND c.deleted_at IS NULL
SET t.car_id = c.id
WHERE t.car_id IS NULL;

---

UPDATE nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t
LEFT JOIN nascar.cars_drivers cd
    ON cd.car_id = t.car_id
   AND cd.driver_id = t.driver_id
   AND cd.deleted_at IS NULL
SET t.car_driver_id = cd.id
WHERE t.car_driver_id IS NULL;

---

DROP TABLE IF EXISTS nascar.tmp_driver_car_map_{{ week_id }}_{{ table_suffix }};

---

CREATE TABLE nascar.tmp_driver_car_map_{{ week_id }}_{{ table_suffix }} AS
SELECT DISTINCT
    t.driver_id,
    t.driver_name,
    t.car_id,
    t.car_number,
    t.manufacturer_id,
    t.manufacturer_name,
    t.team_id,
    t.team_name,
    t.car_driver_id
FROM nascar.tmp_preload_driver_car_wide_{{ week_id }}_{{ table_suffix }} t;
