-- A vintage default rate is a share and must be within [0, 1].
select vintage, default_rate
from {{ ref('mart_vintage_cohorts') }}
where default_rate < 0 or default_rate > 1
