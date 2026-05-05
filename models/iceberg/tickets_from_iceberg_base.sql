{{ config(
    materialized='table',
    table_format='iceberg'
) }}

select *
from TEST_ETLEAP.MK_DEMO.ZENDESK_TICKETS