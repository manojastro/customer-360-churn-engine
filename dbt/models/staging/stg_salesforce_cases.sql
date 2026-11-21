-- File: dbt/models/staging/stg_salesforce_cases.sql
{{ config(materialized='view') }}

-- 1:1 light transformation over the replicated Salesforce Case object.
-- Normalizes status/priority enums and derives simple support-friction flags.

with source as (

    select * from {{ source('salesforce', 'cases') }}

),

renamed as (

    select
        id::varchar                                    as case_id,
        account_id::varchar                            as account_id,
        contact_id::varchar                            as contact_id,
        case_number                                    as case_number,
        nullif(trim(subject), '')                      as subject,
        coalesce(lower(nullif(trim(status), '')), 'unknown')
                                                       as status,
        coalesce(lower(nullif(trim(priority), '')), 'p3')
                                                       as priority,
        coalesce(lower(nullif(trim(type), '')), 'other')
                                                       as case_type,
        lower(coalesce(reason, 'unspecified'))         as reason_code,
        is_escalated = true                            as is_escalated,
        is_closed = true                               as is_closed,
        satisfaction::number(4, 2)                     as csat_score,
        created_at::timestamp_ntz                      as case_created_at,
        closed_at::timestamp_ntz                       as case_closed_at
    from source

)

select * from renamed
