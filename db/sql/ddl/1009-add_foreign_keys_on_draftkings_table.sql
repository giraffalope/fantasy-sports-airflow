ALTER TABLE `draftkings`
    ADD CONSTRAINT `fk_draftkings_week_id` FOREIGN KEY (`week_id`) REFERENCES `weeks`(`id`) ON DELETE CASCADE;

