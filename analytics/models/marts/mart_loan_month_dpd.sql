-- Per loan-month: the days-past-due bucket and a latching default flag.
-- A loan is in default once it has ever been 90+ DPD, and stays in default
-- thereafter (the window max latches it).
with base as (
    select * from {{ ref('stg_loan_status') }}
)
select
    loan_id,
    origination_month,
    as_of_month,
    days_past_due,
    outstanding_principal_minor,
    case
        when days_past_due = 0 then 'current'
        when days_past_due <= 30 then '1-30'
        when days_past_due <= 60 then '31-60'
        when days_past_due <= 90 then '61-90'
        else '90+'
    end as dpd_bucket,
    max(case when days_past_due > 90 then 1 else 0 end) over (
        partition by loan_id
        order by as_of_month
        rows between unbounded preceding and current row
    ) = 1 as is_default
from base
