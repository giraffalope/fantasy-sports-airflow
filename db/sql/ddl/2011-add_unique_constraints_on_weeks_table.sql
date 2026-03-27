ALTER TABLE `weeks`
  ADD UNIQUE KEY `unique_weeks_season_track_number` (`season_id`, `track_id`, `number`);
  