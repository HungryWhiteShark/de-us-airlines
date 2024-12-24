select *
from {{ source('gold', 'cancellation_codes') }}