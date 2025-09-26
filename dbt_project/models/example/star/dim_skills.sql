with raw as (
    select *
    from {{ ref('stg_job_postings') }}
),

soft as (
    select
        trim(skill) as skill_name,
        'Soft' as skill_type
    from raw
    cross join unnest(string_split(soft_skills, ',')) as t(skill)
    where soft_skills is not null
      and trim(skill) <> ''
),

hard as (
    select
        trim(skill) as skill_name,
        'Hard' as skill_type
    from raw
    cross join unnest(string_split(hard_skills, ',')) as t(skill)
    where hard_skills is not null
      and trim(skill) <> ''
),

all_skills as (
    select * from soft
    union all
    select * from hard
)

select distinct
    md5(skill_name) as skill_id,
    skill_name,
    skill_type
from all_skills
