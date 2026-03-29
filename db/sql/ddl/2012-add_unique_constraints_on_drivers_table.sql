ALTER TABLE `drivers`
  ADD UNIQUE KEY `unique_drivers_name_parts` (`first_name`, `last_name`, `suffix`);
