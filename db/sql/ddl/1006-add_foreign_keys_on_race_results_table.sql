ALTER TABLE `race_results`
    ADD CONSTRAINT `fk_race_results_race_id` FOREIGN KEY (`race_id`) REFERENCES `races`(`id`) ON DELETE CASCADE,
    ADD CONSTRAINT `fk_race_results_car_driver_id` FOREIGN KEY (`car_driver_id`) REFERENCES `cars_drivers`(`id`) ON DELETE CASCADE;

