ALTER TABLE `seasons`
    ADD CONSTRAINT `fk_seasons_series_id` FOREIGN KEY (`series_id`) REFERENCES `series`(`id`) ON DELETE CASCADE;

