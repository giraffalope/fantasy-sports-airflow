ALTER TABLE `qualifyings`
    ADD CONSTRAINT `fk_qualifyings_week_id` FOREIGN KEY (`week_id`) REFERENCES `weeks`(`id`) ON DELETE CASCADE;
