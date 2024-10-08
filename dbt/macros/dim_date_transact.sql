{% macro dim_date_transact(
        start_date,
        end_date
    ) %}
SELECT
    CAST(DATEADD(DAY, rn - 1, '{{ start_date }}') AS DATE) AS d
FROM
    (
        SELECT
            top (
                DATEDIFF(
                    DAY,
                    '{{ start_date }}',
                    '{{ end_date }}'
                )
            ) ROW_NUMBER() over (
                ORDER BY
                    s1.[object_id]
            ) AS rn
        FROM
            sys.all_objects AS s1
            CROSS JOIN sys.all_objects AS s2
        ORDER BY
            s1.[object_id]
    ) AS system_row
{% endmacro %}
