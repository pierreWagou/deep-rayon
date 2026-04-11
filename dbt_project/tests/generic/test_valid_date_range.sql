-- tests/generic/test_valid_date_range.sql
-- Custom generic test: date column must be within a reasonable range

{% test valid_date_range(model, column_name, min_date='2020-01-01', max_date=None) %}

select {{ column_name }}
from {{ model }}
where
    {{ column_name }} < '{{ min_date }}'::date
    {% if max_date %}
    or {{ column_name }} > '{{ max_date }}'::date
    {% else %}
    or {{ column_name }} > current_date + interval '1 day'
    {% endif %}

{% endtest %}
