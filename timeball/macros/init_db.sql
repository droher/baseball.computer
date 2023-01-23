{% macro init_db(args) %}
  {% set base_url = "https://baseball.computer" %}

  {% set sql %}
    {% for node in graph.sources.values() if node.schema == "misc" %}
        {% set prefix = "simple" if node.schema == "misc" else "event" %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.name }}.parquet');
    {% endfor %}
  {% endset %}

{% do log(sql, info=True)%}
{% do run_query(sql) %}
{% endmacro %}

{% macro init_db_csv_rust(args) %}
  {% set csv_dir = "/Users/davidroher/Repos/boxball-rs/data" %}

  {% set sql %}
    {% for node in graph.sources.values() if node.schema != 'misc' %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (SELECT * FROM read_csv('{{ csv_dir }}/{{ node.name }}.csv', header=True, auto_detect=True));
    {% endfor %}
  {% endset %}

{% do log(sql, info=True)%}
{% do run_query(sql) %}
{% endmacro %}