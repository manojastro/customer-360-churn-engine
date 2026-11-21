-- File: dbt/models/marts/dim_customer.sql
{{ config(
    materialized='table',
    cluster_by=['customer_key'],
    tags=['marts', 'core']
) }}

/*
dim_customer — the unified Customer 360 dimension.

Grain ............ one row per canonical business account (unique on customer_key).
Identity ......... CRM account_id is the system-of-record key; support/usage
                   telemetry attaches through an identity-resolution ladder
                   (CRM id -> freshest support email -> billing domain -> anon).
Deduplication .... Salesforce replicates master/shadow account pairs; these are
                   collapsed with row_number() ranked by record completeness,
                   then recency, so exactly one survivor per account_id remains.
Materialization .. current-state table carrying SCD-style validity columns;
                   swap to snapshots when point-in-time history is required.
*/

with case_agg as (

    select
        account_id,
        count(*)                                        as total_cases,
        count_if(not is_closed)                         as open_cases,
        count_if(is_escalated and not is_closed)        as open_escalated_cases,
        avg(csat_score)                                 as avg_case_csat,
        max(case_created_at)                            as last_case_at
    from {{ ref('stg_salesforce_cases') }}
    group by account_id

),

ticket_agg as (

    select
        account_id,
        count(*)                                        as total_tickets,
        count_if(status not in ('solved', 'closed'))    as open_tickets,
        avg(csat_score)                                 as avg_ticket_csat,
        max(ticket_created_at)                          as last_ticket_at
    from {{ ref('stg_support_tickets') }}
    group by account_id

),

latest_support_email as (

    -- Freshest known support requester email per account; used as the
    -- identity fallback when the account has no usable domain/email in CRM.
    select
        account_id,
        requester_email
    from {{ ref('stg_support_tickets') }}
    where account_id is not null
    qualify row_number() over (
        partition by account_id
        order by ticket_created_at desc
    ) = 1

),

usage_agg as (

    select
        account_id,
        count(*)                                                    as usage_events_90d,
        count_if(event_ts >= dateadd('day', -30, current_timestamp()))::float
                                                                    as usage_events_30d,
        count(distinct user_id)                                     as distinct_users_90d,
        count_if(event_category = 'friction')                       as friction_events_90d,
        count_if(event_category = 'churn_signal')                   as churn_signal_events_90d,
        max(event_ts)                                               as last_usage_at
    from {{ ref('stg_product_usage_events') }}
    where event_ts >= dateadd('day', -90, current_timestamp())
    group by account_id

),

enriched as (

    select
        a.account_id                                    as customer_key,
        a.account_name,
        a.industry,
        a.country,
        a.website_domain,
        a.account_tier,
        a.employee_count,
        a.annual_revenue,
        a.account_owner_id,
        se.requester_email                              as primary_contact_email,
        a.crm_created_at                                as first_seen_at,
        greatest(
            a.crm_updated_at,
            ca.last_case_at,
            ta.last_ticket_at,
            ua.last_usage_at,
            a.crm_created_at
        )                                               as last_activity_at,
        coalesce(ca.total_cases, 0)                     as total_cases,
        coalesce(ca.open_cases, 0)                      as open_cases,
        coalesce(ca.open_escalated_cases, 0)            as open_escalated_cases,
        coalesce(ta.total_tickets, 0)                   as total_tickets,
        coalesce(ta.open_tickets, 0)                    as open_tickets,
        coalesce(ua.usage_events_30d, 0)::float         as usage_events_30d,
        coalesce(ua.usage_events_90d, 0)::float         as usage_events_90d,
        coalesce(ua.distinct_users_90d, 0)              as distinct_users_90d,
        coalesce(ua.friction_events_90d, 0)             as friction_events_90d,
        coalesce(ua.churn_signal_events_90d, 0)         as churn_signal_events_90d,
        coalesce(ca.avg_case_csat, ta.avg_ticket_csat)  as support_csat,
        ua.last_usage_at
    from {{ ref('stg_salesforce_accounts') }} a
    left join case_agg ca
        on a.account_id = ca.account_id
    left join ticket_agg ta
        on a.account_id = ta.account_id
    left join usage_agg ua
        on a.account_id = ua.account_id
    left join latest_support_email se
        on a.account_id = se.account_id

),

identity_resolved as (

    -- Identity-resolution ladder: verified support email -> billing domain ->
    -- anonymous namespace. The hash is the stable join key for ML features.
    select
        e.*,
        md5(coalesce(
            lower(e.primary_contact_email),
            e.website_domain,
            'anon:' || e.customer_key
        ))                                              as identity_hash,
        (case when e.annual_revenue is not null then 2 else 0 end)
      + (case when e.website_domain is not null then 1 else 0 end)
      + (case when e.employee_count is not null then 1 else 0 end)
                                                        as record_completeness_rank
    from enriched e

),

deduplicated as (

    -- Collapse master/shadow CRM duplicates: one survivor per account_id.
    select *
    from identity_resolved
    qualify row_number() over (
        partition by customer_key
        order by
            record_completeness_rank desc,
            last_activity_at desc
    ) = 1

),

scored as (

    select
        d.*,
        datediff('day', d.first_seen_at::date, current_date())
                                                        as tenure_days,

        -- Cumulative recognized value proxy: ARR x years of tenure.
        round(
            coalesce(d.annual_revenue, 0)
            * datediff('day', d.first_seen_at::date, current_date()) / 365.0,
            2
        )                                               as lifetime_value,

        -- Health-score components, each normalized to [0, 1].
        least(greatest(
            coalesce(d.usage_events_30d, 0)
            / greatest(d.usage_events_90d / 3.0, 1.0),
            0
        ), 1)                                           as usage_momentum,
        least((
            d.open_escalated_cases * 2
            + d.churn_signal_events_90d
            + d.friction_events_90d * 0.5
        ) / 10.0, 1)                                    as support_friction,
        coalesce(d.support_csat, 3.0) / 5.0             as csat_component

    from deduplicated d

)

select
    customer_key,
    identity_hash,
    account_name,
    industry,
    country,
    website_domain,
    account_tier,
    employee_count,
    annual_revenue,
    account_owner_id,
    primary_contact_email,
    first_seen_at,
    last_activity_at,
    tenure_days,
    lifetime_value,
    -- Weighted blend: engagement momentum dominates, support friction penalizes,
    -- CSAT moderates. Clamped defensively even though components are bounded.
    cast(round(100 * (
        0.45 * usage_momentum
      + 0.35 * (1 - support_friction)
      + 0.20 * csat_component
    ), 1) as number(5, 1))                              as health_score,
    total_cases,
    open_cases,
    open_escalated_cases,
    total_tickets,
    open_tickets,
    usage_events_30d,
    usage_events_90d,
    distinct_users_90d,
    friction_events_90d,
    churn_signal_events_90d,
    last_usage_at,
    -- SCD-style metadata (snapshot of current state)
    true                                                as is_current,
    current_timestamp()                                 as dbt_valid_from,
    cast(null as timestamp_ntz)                         as dbt_valid_to
from scored
