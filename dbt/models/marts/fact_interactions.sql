-- File: dbt/models/marts/fact_interactions.sql
{{
    config(
        materialized='incremental',
        unique_key='interaction_key',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['interaction_ts'],
        tags=['marts', 'core']
    )
}}

/*
fact_interactions - unified interaction fact.

Grain ............ exactly one row per customer-facing event: a CRM case,
                   a help-desk ticket, or a product usage event.
Surrogate key .... md5(source_system || '|' || event_type || '|' || source_event_id)
Incrementality ... watermark pushdown into every source branch with a
                   configurable overlap window so in-place source updates are
                   re-emitted and absorbed by the MERGE; the trailing QUALIFY
                   guarantees each emitted batch is duplicate-free.
*/

{% set overlap_days = var('fact_overlap_days', 3) %}

with watermark as (

    {% if is_incremental() %}
    select coalesce(max(interaction_ts), '1900-01-01'::timestamp_ntz) as wm
    from {{ this }}
    {% else %}
    select '1900-01-01'::timestamp_ntz as wm
    {% endif %}

),

cases as (

    select
        'salesforce'                                   as source_system,
        'support_case'                                 as event_type,
        c.case_id                                      as source_event_id,
        c.account_id                                   as customer_key,
        c.subject                                      as interaction_summary,
        c.status                                       as event_status,
        case when c.is_escalated then 'negative'
             when c.csat_score >= 4 then 'positive'
             else 'neutral' end                        as sentiment_class,
        c.case_created_at                              as interaction_ts
    from {{ ref('stg_salesforce_cases') }} c
    cross join watermark w
    where c.case_created_at >= w.wm - interval '{{ overlap_days }} days'

),

tickets as (

    select
        'support_desk'                                 as source_system,
        'support_ticket'                               as event_type,
        t.ticket_id                                    as source_event_id,
        t.account_id                                   as customer_key,
        t.subject                                      as interaction_summary,
        t.status                                       as event_status,
        case when t.priority = 'urgent' then 'negative'
             when t.csat_score >= 4 then 'positive'
             else 'neutral' end                        as sentiment_class,
        t.ticket_created_at                            as interaction_ts
    from {{ ref('stg_support_tickets') }} t
    cross join watermark w
    where t.ticket_created_at >= w.wm - interval '{{ overlap_days }} days'

),

usage_events as (

    select
        'product'                                      as source_system,
        case u.event_category
            when 'engagement'   then 'product_usage_engagement'
            when 'value_action' then 'product_usage_value_action'
            when 'friction'     then 'product_usage_friction'
            when 'churn_signal' then 'product_usage_churn_signal'
            else 'product_usage_other'
        end                                            as event_type,
        u.usage_event_id                               as source_event_id,
        u.account_id                                   as customer_key,
        u.event_name                                   as interaction_summary,
        cast(null as varchar)                          as event_status,
        case when u.event_category in ('friction', 'churn_signal') then 'negative'
             when u.event_category = 'value_action' then 'positive'
             else 'neutral' end                        as sentiment_class,
        u.event_ts                                     as interaction_ts
    from {{ ref('stg_product_usage_events') }} u
    cross join watermark w
    where u.event_ts >= w.wm - interval '{{ overlap_days }} days'

),

unified as (

    select
        md5(source_system || '|' || event_type || '|' || source_event_id)
                                                       as interaction_key,
        source_system,
        event_type,
        source_event_id,
        customer_key,
        interaction_summary,
        event_status,
        sentiment_class,
        interaction_ts,
        current_timestamp()                            as _dbt_loaded_at
    from cases

    union all

    select
        md5(source_system || '|' || event_type || '|' || source_event_id),
        source_system,
        event_type,
        source_event_id,
        customer_key,
        interaction_summary,
        event_status,
        sentiment_class,
        interaction_ts,
        current_timestamp()
    from tickets

    union all

    select
        md5(source_system || '|' || event_type || '|' || source_event_id),
        source_system,
        event_type,
        source_event_id,
        customer_key,
        interaction_summary,
        event_status,
        sentiment_class,
        interaction_ts,
        current_timestamp()
    from usage_events

)

select *
from unified
qualify row_number() over (
    partition by interaction_key
    order by interaction_ts desc
) = 1
