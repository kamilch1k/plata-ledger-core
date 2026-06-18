-- Portfolio-at-risk (PAR30) by month: share of outstanding principal that is
-- more than 30 days past due.
select
    as_of_month,
    sum(outstanding_principal_minor) as total_outstanding_minor,
    sum(case when days_past_due > 30 then outstanding_principal_minor else 0 end) as at_risk_minor,
    round(
        sum(case when days_past_due > 30 then outstanding_principal_minor else 0 end) * 1.0
        / nullif(sum(outstanding_principal_minor), 0),
        4
    ) as par30
from {{ ref('mart_loan_month_dpd') }}
group by as_of_month
order by as_of_month
