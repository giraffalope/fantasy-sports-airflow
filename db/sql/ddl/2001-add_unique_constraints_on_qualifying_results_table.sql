ALTER TABLE `qualifying_results`
  ADD UNIQUE KEY `unique_qualifying_results_qualifying_car_driver` (`qualifying_id`, `car_driver_id`),
  ADD UNIQUE KEY `unique_qualifying_results_qualifying_finish_position` (`qualifying_id`, `finish_position`);
