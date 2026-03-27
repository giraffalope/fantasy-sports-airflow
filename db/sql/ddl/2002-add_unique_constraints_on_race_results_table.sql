ALTER TABLE `race_results`
  ADD UNIQUE KEY `unique_race_results_race_car_driver` (`race_id`, `car_driver_id`),
  ADD UNIQUE KEY `unique_race_results_race_finish_position` (`race_id`, `finish_position`);
