with source as (
    select * from {{ source('raw', 'raw_job_postings') }}
),

loc_trim as (
    select *,
        trim(coalesce("location", '')) as _loc_trim
    from source
),

cleaned as (
    select
        trim("company") as company_name,
        nullif(trim(coalesce("company url", '')), '') as company_url,

        -- split from right to left: country = last token, region = second-last, city = third-last
        nullif(trim(reverse(split_part(reverse(_loc_trim), ',', 3))), '') as city,
        nullif(trim(reverse(split_part(reverse(_loc_trim), ',', 2))), '') as governorate,
        nullif(trim(reverse(split_part(reverse(_loc_trim), ',', 1))), '') as country,

        nullif(trim(coalesce("job_link", '')), '') as job_url,
        trim("job title") as job_title,
        
        -- drop rows where job description is only whitespace
        nullif(trim(coalesce("job description", '')), '') as job_description,

        trim("employment type") as employment_type,

        case
            when upper(trim(coalesce("job flexibility", ''))) = 'UNDEFINED' then 'Not Specified'
            when trim(coalesce("job flexibility", '')) = '' then 'Not Specified'
            else trim("job flexibility")
        end as job_flexibility,

        current_date as posting_date,
        "hard_skills",
        "soft_skills",
    from loc_trim
    where nullif(trim(coalesce("job description", '')), '') is not null
)

select * from cleaned