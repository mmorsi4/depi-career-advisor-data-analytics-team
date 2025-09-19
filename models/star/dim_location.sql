with deduped as (
    select
        coalesce(city, '') as city,
        coalesce(governorate, '') as governorate,
        coalesce(country, '') as country,
        row_number() over (
            partition by city, governorate, country
            order by city, governorate
        ) as rn
    from {{ ref('stg_job_postings') }}
)

select
    md5(coalesce(city, '') || coalesce(governorate, '') || coalesce(country, '')) as location_id,
    city,
    governorate,
    country
from deduped
where rn = 1
