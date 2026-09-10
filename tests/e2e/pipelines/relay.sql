INSERT INTO {{ sink('dbx', 'orders_out') }}
select id, name
from {{ source('dbx', 'orders_in') }}
order by id
