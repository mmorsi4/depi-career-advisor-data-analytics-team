with deduped as (
    select
        *,
        row_number() over (
            partition by
                job_title,
                company_name,
                coalesce(city, ''),
                coalesce(governorate, ''),
                coalesce(country, ''),
                posting_date
            order by job_description desc nulls last
        ) as rn
    from {{ ref('stg_job_postings') }}
)

select
    md5(job_title || company_name || coalesce(city, '') || coalesce(governorate, '') || coalesce(country, '')) as posting_id,
    posting_date as date_id,
    md5(company_name) as company_id,
    md5(coalesce(city, '') || coalesce(governorate, '') || coalesce(country, '')) as location_id,
    job_url,
    job_title,
    job_description,
    employment_type,
    job_flexibility
from deduped
where rn = 1
