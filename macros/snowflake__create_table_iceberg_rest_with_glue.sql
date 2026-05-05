{% macro snowflake__create_table_iceberg_rest_with_glue(relation, compiled_code, catalog_relation) -%}

{%- set contract_config = config.get('contract') -%}
{%- set copy_grants = config.get('copy_grants', default=false) -%}
{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set partition_by_keys = get_partition_by_keys(catalog_relation) -%}
{%- if partition_by_keys -%}
  {%- set partition_by_keys_quotes = [] -%}
  {%- for key in partition_by_keys -%}
    {% set quoted_key = '"' ~ key.lower() ~ '"' %}
    {%- do partition_by_keys_quotes.append(quoted_key) -%}
  {%- endfor -%}
  {%- set partition_by_string = partition_by_keys_quotes | join(", ") -%}
{% else %}
  {%- set partition_by_string = none -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

{% set existing_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
{% if existing_relation %}
    drop table if exists {{ existing_relation }};
{% endif %}

create iceberg table {{ relation }} (
    {%- if contract_config.enforced -%}
        {#-- Use contract-defined types directly; they already specify the correct Iceberg types (long, string, etc.) --#}
        {%- for column_name, column in model.columns.items() -%}
            {{ adapter.quote(column_name.lower()) }} {{ column.data_type | upper }}
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    {%- else -%}
        {%- set sql_columns = get_column_schema_from_query(compiled_code) -%}
        {%- for column in sql_columns -%}
            {%- if column.data_type == "FIXED" -%}
                {#-- source precision reflects column definition, not max data value; default to LONG to be safe --#}
                {%- if column.numeric_precision is not none and column.numeric_scale == 0 and column.numeric_precision <= 9 -%}
                    {%- set data_type = "INT" -%}
                {%- else -%}
                    {%- set data_type = "LONG" -%}
                {%- endif -%}
            {%- elif "character varying" in column.data_type -%}
                {%- set data_type = "STRING" -%}
            {%- else -%}
                {%- set data_type = column.data_type -%}
            {%- endif -%}
            {{ adapter.quote(column.name.lower()) }} {{ data_type }}
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    {%- endif -%}
)
{% if partition_by_string -%} partition by ({{ partition_by_string }}) {%- endif %}
{{ optional('external_volume', catalog_relation.external_volume, "'") }}
{{ optional('target_file_size', catalog_relation.target_file_size, "'") }}
{{ optional('auto_refresh', catalog_relation.auto_refresh) }}
{{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days)}}
{% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
{% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
{% if copy_grants -%} copy grants {%- endif %}
;

insert into {{ relation }}
    {{ compiled_code }};

{%- endmacro %}