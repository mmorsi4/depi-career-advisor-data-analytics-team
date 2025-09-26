with deduped as (
    select
        company_name,
        company_url,
        row_number() over (
            partition by company_name
            order by company_name
        ) as rn
    from {{ ref('stg_job_postings') }}
    where company_name is not null
)

select
    md5(company_name) as company_id,
    company_name,
    company_url
from deduped
where rn = 1
