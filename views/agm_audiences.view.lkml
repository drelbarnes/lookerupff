view: agm_audiences {
  derived_table: {
    sql: with users as (
      select
      user_id
      , email
      , status
      from customers.all_customers
      where report_date = current_date
    )
    , ios as (
      with all_events as (
        select
        user_id
        , context_device_advertising_id as ios_ad_id
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rn
        from ios.identifies
      )
      select user_id, ios_ad_id from all_events where rn = 1
    )
    , android as (
      with all_events as (
        select
        user_id
        , context_device_advertising_id as android_ad_id
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rn
        from android.identifies
      )
      select user_id, android_ad_id from all_events where rn = 1
    )
    , audience as (
      select
      a.user_id
      , a.email
      , a.status
      , b.ios_ad_id
      , c.android_ad_id
      from users a
      left join ios b on a.user_id = b.user_id
      left join android c on a.user_id = c.user_id
      group by 1,2,3,4,5
    )
    select
    user_id
    , sha2(email, 256) as email_sha2
    , case when ios_ad_id is null then null
        when ios_ad_id = '00000000-0000-0000-0000-000000000000' then null
        else sha2(ios_ad_id, 256) end as idfa_sha2
    , case when android_ad_id is null then null
        when android_ad_id = '00000000-0000-0000-0000-000000000000' then null
        else sha2(android_ad_id, 256) end as gaid_sha2
    , status
    -- count(*) as total, count(distinct user_id) as dist_total
    from audience ;;
  }

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: email_sha2 {
    type: string
    sql: ${TABLE}.email_sha2 ;;
  }

  dimension: idfa_sha2 {
    type: string
    sql: ${TABLE}.idfa_sha2 ;;
  }

  dimension: gaid_sha2 {
    type: string
    sql: ${TABLE}.gaid_sha2 ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
}
