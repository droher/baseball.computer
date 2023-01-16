SELECT *, game_id || '-' || event_id AS event_key
FROM event.event_raw
