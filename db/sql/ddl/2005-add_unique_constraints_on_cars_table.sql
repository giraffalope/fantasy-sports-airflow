ALTER TABLE `cars`
  ADD UNIQUE KEY `unique_cars_team_manufacturer_number` (`team_id`, `manufacturer_id`, `number`);
