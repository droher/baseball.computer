{% macro generate_base_model(source_name, table_name) %}
{% set source_id = "source.timeball." ~ source_name ~ "." ~ table_name %}

{% set meta = graph.sources[source_id]['meta']  %}
{% set pk_exp = "concat_ws('-', " ~ ", ".join(meta["primary_keys"]) ~ ")" if meta.get("primary_keys") else None %}
{% set hash_exp = dbt_utils.surrogate_key(graph.sources[source_id]["columns"].keys()) %}
{% set id_exp = meta.get('id_expression') or pk_exp or hash_exp %}
{% set source_string = "{{ source('" ~ source_name ~ "', '" ~ table_name ~ "') }}" %}

{% set sql %}
  WITH source AS (
  SELECT
    {{ id_exp }} AS id,
    *
  FROM {{ source_string }}
)
SELECT * FROM source
{% endset %}%}

{{ log(sql, info=True) }}
{% endmacro %}
