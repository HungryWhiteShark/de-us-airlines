
--select *
--from {{ source('gold', 'airports') }}
select *
from {{ ref('airports') }}




