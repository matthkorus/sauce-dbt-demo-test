 {{
    config(
        post_hook= "{{ copy_to_iceberg(this, 'dbt_test') }}"
    )
}}



select
  $1::varchar as "a",
  $2::varchar as "b"
from values (
  'a', 'b' ), (
  'c', 'd' )