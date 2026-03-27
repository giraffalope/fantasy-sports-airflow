ALTER TABLE `draftkings_salaries`
    ADD CONSTRAINT `fk_draftkings_salaries_draftkings_id` FOREIGN KEY (`draftkings_id`) REFERENCES `draftkings`(`id`) ON DELETE CASCADE,
    ADD CONSTRAINT `fk_draftkings_salaries_car_driver_id` FOREIGN KEY (`car_driver_id`) REFERENCES `cars_drivers`(`id`) ON DELETE CASCADE;
