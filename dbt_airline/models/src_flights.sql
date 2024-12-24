select *
from {{ source('gold', 'flights') }}