-- tests/generic/test_positive_value.sql
-- Custom generic test: column must contain only positive values (or zero)

{% test positive_value(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
