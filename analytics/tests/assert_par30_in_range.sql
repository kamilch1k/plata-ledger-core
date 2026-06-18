-- PAR30 is a ratio and must be within [0, 1].
select as_of_month, par30
from {{ ref('mart_portfolio_at_risk') }}
where par30 < 0 or par30 > 1
