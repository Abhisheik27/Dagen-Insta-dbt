{%- macro generate_surrogate_key(column_list) -%}
    TO_HEX(MD5(CONCAT(
        {%- for col in column_list -%}
            CAST({{ col }} AS STRING)
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    )))
{%- endmacro -%}