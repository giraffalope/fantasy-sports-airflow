INSERT IGNORE INTO `nascar`.`series` (name)
VALUES ('{{ series_name }}')
;
---
INSERT IGNORE INTO `nascar`.`seasons` (series_id, year)
SELECT id, {{ data_interval_start.year }}
FROM `nascar`.`series` s
WHERE s.name = '{{ series_name }}'
;