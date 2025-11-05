{{ config(
	tags= ["snowflake"]
    ) }} 

select
  $1::varchar as "a",
  $2::varchar as "b"
from values (
  'a', 'b' ), (
  'c', 'd' )