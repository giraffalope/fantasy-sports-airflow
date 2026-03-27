ALTER TABLE `cars_drivers`
    ADD CONSTRAINT `fk_cars_drivers_car_id` FOREIGN KEY (`car_id`) REFERENCES `cars`(`id`) ON DELETE CASCADE,
    ADD CONSTRAINT `fk_cars_drivers_driver_id` FOREIGN KEY (`driver_id`) REFERENCES `drivers`(`id`) ON DELETE CASCADE;
