{#
{{ config( materialized='incremental',unique_key = 'id') }}
#}

/*
SELECT 1 AS id, 'hello' AS messag
*/ 


select 1 as id, 'bye' as message


