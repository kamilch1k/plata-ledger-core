-- Each loan should appear once per month in the DPD mart.
select loan_id, as_of_month, count(*) as n
from {{ ref('mart_loan_month_dpd') }}
group by loan_id, as_of_month
having count(*) > 1
