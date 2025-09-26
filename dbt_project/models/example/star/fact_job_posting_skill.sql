with raw as (
    select *
    from {{ ref('stg_job_postings') }}
),

soft as (
    select
        md5(job_title || company_name || coalesce(city, '') || coalesce(governorate, '') || coalesce(country, '')) as posting_id,
        md5(trim(skill)) as skill_id
    from raw
    cross join unnest(string_split(soft_skills, ',')) as t(skill)
    where soft_skills is not null
      and trim(skill) <> ''
),

hard as (
    select
        md5(job_title || company_name || coalesce(city, '') || coalesce(governorate, '') || coalesce(country, '')) as posting_id,
        md5(trim(skill)) as skill_id
    from raw
    cross join unnest(string_split(hard_skills, ',')) as t(skill)
    where hard_skills is not null
      and trim(skill) <> ''
),

all_links as (
    select * from soft
    union all
    select * from hard
)

select distinct
    posting_id,
    skill_id
from all_links