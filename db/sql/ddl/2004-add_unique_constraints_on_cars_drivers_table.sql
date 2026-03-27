ALTER TABLE `cars_drivers`
  ADD UNIQUE KEY `unique_cars_drivers_car_driver` (`car_id`, `driver_id`);
