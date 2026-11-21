-- File: dbt/models/staging/stg_support_tickets.sql
{{ config(materialized='view') }}

-- 1:1 light transformation over replicated support-desk tickets (e.g. Zendesk).
-- Normalizes channel/satisfaction enums; requester_email is the natural join key
-- used downstream for identity resolution when no CRM account id exists.

with source as (

    select * from {{ source('support', 'tickets') }}

),

renamed as (

    select
        ticket_id::varchar                             as ticket_id,
        account_id::varchar                            as account_id,
        lower(nullif(trim(requester_email), ''))       as requester_email,
        nullif(trim(subject), '')                      as subject,
        coalesce(lower(nullif(trim(status), '')), 'unknown')
                                                       as status,
        coalesce(lower(nullif(trim(priority), '')), 'normal')
                                                       as priority,
        coalesce(lower(nullif(trim(channel), '')), 'unknown')
                                                       as channel,
        satisfaction_rating::number(4, 2)              as csat_score,
        created_at::timestamp_ntz                      as ticket_created_at,
        solved_at::timestamp_ntz                       as ticket_solved_at
    from source

)

select * from renamed
