{% macro init_db(args) %}
  {% set base_url = "https://baseball.computer" %}

  {% set sql %}
    {% for node in graph.sources.values() %}
      {% set prefix = node.schema if node.schema in ("misc", "baseballdatabank") else "event" %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (
        SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.name }}.parquet'
      );
      {% for col_name, col_data in node.columns.items() if col_data.get("data_type") %}
        ALTER TABLE {{ node.schema }}.{{ node.name }} ALTER COLUMN {{ col_name }} TYPE {{ col_data.data_type }};
      {% endfor %}
    {% endfor %}
  {% endset %}

{% do log(sql, info=True)%}
{% do run_query(sql) %}

{% endmacro %}

{% macro init_db_sample(args) %}
  {% set base_url = "https://baseball.computer" %}

  {% set sql %}
    {% for node in graph.sources.values() %}
      {% set prefix = node.schema if node.schema in ("misc", "baseballdatabank") else "event" %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (
        SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.name }}.parquet'
        {% if node.schema == "event" %}
          WHERE HASH(event_key // 255) % 10 = 0
        {% endif %}
      );
      {% for col_name, col_data in node.columns.items() if col_data.get("data_type") %}
        ALTER TABLE {{ node.schema }}.{{ node.name }} ALTER COLUMN {{ col_name }} TYPE {{ col_data.data_type }};
      {% endfor %}
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
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (
        SELECT * FROM read_csv('{{ csv_dir }}/{{ node.name }}.csv', header=True, auto_detect=True)
      );
    {% endfor %}
  {% endset %}

{% do log(sql, info=True)%}
{% do run_query(sql) %}
{% endmacro %}