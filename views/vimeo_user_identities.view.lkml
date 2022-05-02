view: vimeo_user_identities {
  derived_table: {
    sql:
      with active_users as (
        with pe as (
          select
          user_id
          , subscription_status as status
          , timestamp
          , platform
          , row_number() over (partition by user_id order by timestamp desc) as rn
          from http_api.purchase_event
        )
        select
        user_id
        , status
        , platform
        , timestamp
        from pe
        where rn = 1
        and status in ("free_trial", "enabled")
      )
      , site_identifies as (
        select
        user_id
        , email
        , phone
        , context_ip
        , anonymous_id
        , context_traits_cross_domain_id
        , "web" as platform
        FROM javascript_upff_home.identifies
        group by 1,2,3,4,5,6
      )
      , seller_identifies as (
        select
        user_id
        , email
        , '' as phone
        , context_ip
        , anonymous_id
        , context_traits_cross_domain_id
        , "web" as platform
        FROM javascript.identifies
        group by 1,2,3,4,5,6
      )
      , all_identifies as (
        select * from site_identifies
        union all
        select* from seller_identifies
      )
      , identity_resolution as (
        select
        a.timestamp
        , a.user_id
        , a.status
        , a.platform
        , b.email
        , b.phone
        , b.context_ip
        , b.anonymous_id
        , b.context_traits_cross_domain_id
        from active_users as a
        full join all_identifies as b
        on a.user_id = b.user_id
      )
        select distinct * from identity_resolution
        where user_id in (select user_id from active_users)
    ;;
  }

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: context_traits_cross_domain_id {
    type: string
    sql: ${TABLE}.context_traits_cross_domain_id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: phone {
    type: string
    sql: ${TABLE}.phone ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }
}
