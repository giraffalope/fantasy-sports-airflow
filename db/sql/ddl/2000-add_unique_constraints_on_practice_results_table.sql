ALTER TABLE `practice_results`
  ADD UNIQUE KEY `unique_practice_results_practice_car_driver` (`practice_id`, `car_driver_id`),
  ADD UNIQUE KEY `unique_practice_results_practice_finish_position` (`practice_id`, `finish_position`);
