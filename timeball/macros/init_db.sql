{% macro init_db(args) %}
  {% set base_url = "https://baseball.computer" %}
  {% set parquet_dir = "/Users/davidroher/Repos/timeball-dbt/parquet" %}

  {% set sql %}
    {% for node in graph.sources.values() %}
        {% set prefix = "simple" if node.schema == "misc" else "event" %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.name }}.parquet');
    {% endfor %}
  {% endset %}

{% do log(sql, info=True)%}
{% do run_query(sql) %}
{% endmacro %}