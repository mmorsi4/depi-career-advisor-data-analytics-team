select distinct
    posting_date as date_id,
    extract(year from posting_date) as year,
    extract(month from posting_date) as month,
    extract(day from posting_date) as day
from {{ ref('stg_job_postings') }}