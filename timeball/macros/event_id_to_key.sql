{%- macro event_id_to_key(event_id, game_event_key) -%}
  {{ game_event_key }} // 255 * 255 + {{ event_id }}
{%- endmacro -%}

{%- macro event_key_to_id(event_key) -%}
  {{ event_key }} - ({{ event_key }} // 255)
{%- endmacro -%}
