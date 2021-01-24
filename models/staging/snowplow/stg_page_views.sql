-- NOTE:
-- The following incremental model was used in the incremental models demo.
-- Use this as a starting place for implementing an incremental model in a 
-- future dbt project with real event data!  

-- In this free, public version of the course, there is not currently a means
-- for exposing this live event data for practice.  If you have similar event
-- data in your data warehouse, feel free to modify the SQL below and apply
-- it to your use case.
{{
    config(
        materialized = 'incremental',
        unique_key = 'page_view_id'
    )
}}

with events as (

    select * from {{ source('snowplow', 'events') }}

    {% if is_incremental() %}
    where collector_tstamp >= (select max(max_collector_tstamp) from {{ this }})
    {% endif %}

),

page_views as (

    select *
    
    from events
    
    where event = 'page_view'

),

aggregated_page_events as (

    select
        page_view_id,
        count(*) * 10 as approx_time_on_page,
        min(derived_tstamp) as page_view_start,
        max(collector_tstamp) as max_collector_tstamp

    from events

    group by 1

),

joined as (
    
    select *

    from page_views
    left join aggregated_page_events using (page_view_id)

)

select * from joined