-- Typed, cleaned view over the raw loan-status snapshots.
select
    loan_id,
    cast(origination_month as date)             as origination_month,
    cast(as_of_month as date)                   as as_of_month,
    cast(days_past_due as integer)              as days_past_due,
    cast(outstanding_principal_minor as bigint) as outstanding_principal_minor
from {{ ref('seed_loan_status') }}
