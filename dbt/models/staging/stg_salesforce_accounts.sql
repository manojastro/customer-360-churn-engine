-- File: dbt/models/staging/stg_salesforce_accounts.sql
{{ config(materialized='view') }}

-- 1:1 light transformation over the replicated Salesforce Account object.
-- Note: this layer intentionally does NOT enforce uniqueness on account_id;
-- duplicate CRM records (master/shadow accounts) are resolved in dim_customer.

with source as (

    select * from {{ source('salesforce', 'accounts') }}

),

renamed as (

    select
        id::varchar                                    as account_id,
        nullif(trim(name), '')                         as account_name,
        lower(nullif(trim(website_domain), ''))        as website_domain,
        coalesce(lower(nullif(trim(industry), '')), 'unknown')
                                                       as industry,
        coalesce(lower(nullif(trim(country), '')), 'unknown')
                                                       as country,
        annual_revenue::number(18, 2)                  as annual_revenue,
        employee_count::number(10, 0)                  as employee_count,
        coalesce(lower(nullif(trim(account_tier), '')), 'unclassified')
                                                       as account_tier,
        owner_id::varchar                              as account_owner_id,
        created_at::timestamp_ntz                      as crm_created_at,
        updated_at::timestamp_ntz                      as crm_updated_at
    from source
    where is_deleted = false

)

select * from renamed
