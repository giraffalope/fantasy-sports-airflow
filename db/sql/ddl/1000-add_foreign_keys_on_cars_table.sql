ALTER TABLE `cars`
    ADD CONSTRAINT `fk_cars_team_id` FOREIGN KEY (`team_id`) REFERENCES `teams`(`id`) ON DELETE CASCADE,
    ADD CONSTRAINT `fk_cars_manufacturer_id` FOREIGN KEY (`manufacturer_id`) REFERENCES `manufacturers`(`id`) ON DELETE CASCADE;
