{#
{{ config( materialized='incremental',unique_key = 'id') }}
#}

/*
SELECT 1 AS id, 'hello' AS message
*/ 


select 1 as id, 'bye' as message


