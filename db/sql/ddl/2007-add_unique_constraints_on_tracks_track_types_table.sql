ALTER TABLE `tracks_track_types`
  ADD UNIQUE KEY `unique_tracks_track_types_track_track_type` (`track_id`, `track_type_id`);
