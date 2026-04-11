-- tests/generic/test_sign_consistency.sql
-- Custom generic test: quantity and spend must have the same sign direction
-- This catches the known data quality issue where quantity is positive but spend is negative

{% test sign_consistency(model, column_name, compare_column) %}

select
    {{ column_name }},
    {{ compare_column }}
from {{ model }}
where
    sign({{ column_name }}) != sign({{ compare_column }})
    and {{ column_name }} != 0
    and {{ compare_column }} != 0

{% endtest %}
