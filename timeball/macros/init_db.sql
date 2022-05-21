{% macro init_db(args) %}
  {% set base_url = "https://boxball.s3.us-west-002.backblazeb2.com/timeball-site" %}
  {% set parquet_dir = "../parquet" %}

  {% set sql %}
    INSTALL 'httpfs';
    LOAD 'httpfs';

    {% for node in graph.sources.values() %}
      CREATE VIEW {{ node.name }} AS (SELECT * FROM '{{ parquet_dir }}/{{ node.name }}.parquet');
    {% endfor %}
  {% endset %}

{% do run_query(sql) %}
{% endmacro %}