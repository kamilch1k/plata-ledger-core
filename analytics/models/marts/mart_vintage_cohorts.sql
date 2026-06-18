-- Default rate by origination vintage: of the loans originated in a month, what
-- share ever defaulted.
with flagged as (
    select
        loan_id,
        origination_month,
        max(case when is_default then 1 else 0 end) as ever_default
    from {{ ref('mart_loan_month_dpd') }}
    group by loan_id, origination_month
)
select
    origination_month                              as vintage,
    count(*)                                       as loans,
    sum(ever_default)                              as defaulted_loans,
    round(sum(ever_default) * 1.0 / count(*), 4)   as default_rate
from flagged
group by origination_month
order by vintage
