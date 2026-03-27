ALTER TABLE `practices`
    ADD CONSTRAINT `fk_practices_week_id` FOREIGN KEY (`week_id`) REFERENCES `weeks`(`id`) ON DELETE CASCADE;
