ALTER TABLE `races`
    ADD CONSTRAINT `fk_races_week_id` FOREIGN KEY (`week_id`) REFERENCES `weeks`(`id`) ON DELETE CASCADE;
