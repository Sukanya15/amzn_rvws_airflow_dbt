{% macro test_productid_format(model, column_name) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE
    NOT (
        LENGTH({{ column_name }}) = 10 AND
        {{ column_name }} ~ '^[A-Z0-9]{10}$'
    )
    AND {{ column_name }} IS NOT NULL

{% endmacro %}