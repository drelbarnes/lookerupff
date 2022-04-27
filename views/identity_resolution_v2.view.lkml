view: identity_resolution_v2 {
  derived_table: {
    sql:
      with enabled_subs as (
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
        timestamp
        , user_id
        , email
        , context_ip
        , anonymous_id
        , context_traits_cross_domain_id
        , "web" as platform
        FROM javascript_upff_home.identifies
        group by 1,2,3,4,5,6,7
      )
      , seller_identifies as (
        select
        timestamp
        , user_id
        , email
        , context_ip
        , anonymous_id
        , context_traits_cross_domain_id
        , "web" as platform
        FROM javascript.identifies
        group by 1,2,3,4,5,6,7
      )
      , all_identifies as (
        select * from site_identifies
        union all
        select* from seller_identifies
      )
      , active_identities as (
        select
        a.timestamp as event_date
        , b.timestamp as identified_at
        , a.user_id
        , a.status
        , a.platform
        , b.email
        , b.context_ip
        , b.anonymous_id
        , b.context_traits_cross_domain_id
        from enabled_subs as a
        full join all_identifies
        on a.user_id = b.user_id
      )
      select distinct * from active_identities
    ;;
  }
}
