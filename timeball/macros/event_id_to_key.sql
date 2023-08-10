{%- macro event_id_to_key(event_id, game_event_key) -%}
  {{ game_event_key }} // 256 * 256 + {{ event_id }}
{%- endmacro -%}