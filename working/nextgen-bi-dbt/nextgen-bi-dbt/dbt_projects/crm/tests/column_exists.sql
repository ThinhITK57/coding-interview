{% test column_exists(model, column_name) %}

select 1
from {{ model }}
limit 0

{% if execute %}
    {% set cols = adapter.get_columns_in_relation(model) %}
    {% set names = cols | map(attribute='name') | map('lower') | list %}

    {% if column_name | lower not in names %}
        select 1
    {% else %}
        select null where false
    {% endif %}
{% endif %}

{% endtest %}