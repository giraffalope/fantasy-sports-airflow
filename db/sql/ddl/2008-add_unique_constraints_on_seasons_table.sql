ALTER TABLE `seasons`
  ADD UNIQUE KEY `unique_seasons_series_year` (`series_id`, `year`);
