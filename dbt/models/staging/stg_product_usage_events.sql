-- File: dbt/models/staging/stg_product_usage_events.sql
{{ config(materialized='view') }}

-- 1:1 light transformation over the replicated product telemetry event stream.
-- Classifies raw event names into a coarse engagement bucket used downstream
-- for churn-signal features; drops synthetic/heartbeat events to cut volume.

with source as (

    select * from {{ source('product', 'usage_events') }}

),

classified as (

    select
        event_id::varchar                              as usage_event_id,
        account_id::varchar                            as account_id,
        user_id::varchar                               as user_id,
        lower(nullif(trim(event_name), ''))            as event_name,
        case
            when lower(event_name) in (
                'login', 'session_start'
            )                                          then 'engagement'
            when lower(event_name) in (
                'feature_used', 'report_generated', 'export_run'
            )                                          then 'value_action'
            when lower(event_name) in (
                'error_raised', 'timeout', 'api_rate_limited'
            )                                          then 'friction'
            when lower(event_name) in (
                'downgrade_click', 'cancel_intent', 'billing_page_viewed'
            )                                          then 'churn_signal'
            else 'other'
        end                                            as event_category,
        session_id::varchar                            as session_id,
        platform::varchar                              as client_platform,
        occurred_at::timestamp_ntz                     as event_ts
    from source

)

select * from classified
where event_name is not null
  and event_name not in ('heartbeat', 'synthetic_ping')
