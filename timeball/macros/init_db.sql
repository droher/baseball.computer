{% macro init_db(args) %}
  {% set base_url = "https://baseball.computer" %}
  {% set parquet_dir = "/Users/davidroher/Repos/timeball-dbt/parquet" %}

  {% set sql %}
    INSTALL 'httpfs';
    LOAD 'httpfs';

    {% for node in graph.sources.values() %}
        {% set prefix = "simple" if node.schema == "misc" else "event" %}
      CREATE OR REPLACE VIEW {{ node.name }} AS (SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.name }}.parquet');
    {% endfor %}
  {% endset %}

{% do run_query(sql) %}
{% endmacro %}