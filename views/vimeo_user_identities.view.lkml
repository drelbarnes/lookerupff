view: vimeo_user_identities {
  derived_table: {
    sql: with web_identities as (
      with web_app as (
        select
        case
          when safe_cast(user_id as string) = safe_cast(anonymous_id as string) then cast(null as string)
          else safe_cast(user_id as string)
        end as user_id
        , safe_cast(user_email as string) as email
        , cast(null as string) as phone
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        FROM javascript.pages
        group by 1,2,3,4,5,6
      )
      , web_site as (
        select
        case
          when safe_cast(a.user_id as string) = safe_cast(a.anonymous_id as string) then cast(null as string)
          else safe_cast(a.user_id as string)
        end as user_id
        , safe_cast(b.email as string) as email
        , safe_cast(b.phone as string) as phone
        , safe_cast(a.context_ip as string) as ip_address
        , safe_cast(a.anonymous_id as string) as anonymous_id
        , safe_cast(a.context_traits_cross_domain_id as string) as cross_domain_id
        FROM javascript_upff_home.pages as a
        left join javascript_upff_home.identifies as b
        on a.anonymous_id = b.anonymous_id
        group by 1,2,3,4,5,6
      )
      , union_all as (
        select * from web_app
        union all
        select * from web_site
      )
      -- The Vimeo OTT Web Segment Implementation bug rendered the above logic useless. Alt pages is the temp fix.
        , alt_pages as (
        select
        case
          when safe_cast(a.user_id as string) = safe_cast(a.anonymous_id as string) then cast(null as string)
          else safe_cast(a.user_id as string)
        end as user_id
        , safe_cast(b.email as string) as email
        , cast(null as string) as phone
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(a.anonymous_id as string) as anonymous_id
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        from ${upff_page_events.SQL_TABLE_NAME} as a
        left join javascript_upff_home.identifies as b
        on a.anonymous_id = b.anonymous_id
      )
      , distinct_identities as (
      select distinct * from alt_pages
      )
      , look_up_existing_user_id as (
        select user_id, email, phone, anonymous_id, cross_domain_id from distinct_identities where (user_id is not null and email is not null)
      )
      , look_up_existing_email as (
        select user_id, email, phone, anonymous_id, cross_domain_id from distinct_identities where (user_id is null and email is not null)
      )
      , distinct_identities_filled as (
        select
        a.*
        , b.user_id as user_id_b
        , b.email as email_b
        , b.phone as phone_b
        , b.cross_domain_id as cross_domain_id_b
        , c.user_id as user_id_c
        , c.email as email_c
        , c.phone as phone_c
        , c.cross_domain_id as cross_domain_id_c
        from distinct_identities as a
        left join look_up_existing_user_id as b
        on a.anonymous_id = b.anonymous_id
        left join look_up_existing_email as c
        on a.anonymous_id = c.anonymous_id
      )
      , unique_identities as (
        with coalesce_columns as (
          select
          coalesce(user_id, user_id_b, user_id_c) as user_id
          , coalesce(email, email_b, email_c) as email
          , coalesce(phone, phone_b, phone_c) as phone
          , ip_address
          , anonymous_id
          , coalesce(cross_domain_id, cross_domain_id_b, cross_domain_id_c) as cross_domain_id
          from distinct_identities_filled
        )
        select *
        , cast(null as string) as device_id
        , "web" as platform
        from coalesce_columns
      )
      select distinct * from unique_identities
    )
    , roku_identities as (
      with screens as (
        select
        case
          when safe_cast(user_id as string) = safe_cast(anonymous_id as string) then cast(null as string)
          else safe_cast(user_id as string)
        end as user_id
        , safe_cast(user_email as string) as email
        , safe_cast(anonymous_id as string) as anonymous_id
        , case
            when safe_cast(device_id as string) = "<tracking disabled>" then cast(null as string)
            else safe_cast(device_id as string)
        end as device_id
        FROM roku.screens
        group by 1,2,3,4
      )
      , distinct_identities as (
        select distinct * from screens
      )
      , look_up_existing_user_id as (
        select user_id, email, anonymous_id, device_id from distinct_identities where (user_id is not null and email is not null)
      )
      , look_up_existing_email as (
        select user_id, email, anonymous_id, device_id from distinct_identities where (user_id is null and email is not null)
      )
      , distinct_identities_filled as (
        select
        a.*
        , b.user_id as user_id_b
        , b.email as email_b
        , b.device_id as device_id_b
        , c.user_id as user_id_c
        , c.email as email_c
        , c.device_id as device_id_c
        from distinct_identities as a
        left join look_up_existing_user_id as b
        on a.anonymous_id = b.anonymous_id
        left join look_up_existing_email as c
        on a.anonymous_id = c.anonymous_id
      )
      , unique_identities as (
        with coalesce_columns as (
          select
          coalesce(user_id, user_id_b, user_id_c) as user_id
          , coalesce(email, email_b, email_c) as email
          , anonymous_id
          , coalesce(device_id, device_id_b, device_id_c) as device_id
          from distinct_identities_filled
        )
        select *
        , "roku" as platform
        , cast(null as string) as phone
        , safe_cast(null as string) as ip_address
        , safe_cast(null as string) as cross_domain_id
        from coalesce_columns
      )
      select distinct * from unique_identities
    )
    , amazon_identities as (
      with screens as (
        select
        case
          when safe_cast(user_id as string) = safe_cast(anonymous_id as string) then cast(null as string)
          else safe_cast(user_id as string)
        end as user_id
        , safe_cast(user_email as string) as email
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        FROM amazon_fire_tv.screens
        group by 1,2,3,4,5
      )
      , distinct_identities as (
        select distinct * from screens
      )
      , look_up_existing_user_id as (
        select user_id, email, anonymous_id, ip_address, device_id from distinct_identities where (user_id is not null and email is not null)
      )
      , look_up_existing_email as (
        select user_id, email, anonymous_id, ip_address, device_id from distinct_identities where (user_id is null and email is not null)
      )
      , distinct_identities_filled as (
        select
        a.*
        , b.user_id as user_id_b
        , b.email as email_b
        , b.ip_address as ip_address_b
        , b.device_id as device_id_b
        , c.user_id as user_id_c
        , c.email as email_c
        , c.ip_address as ip_address_c
        , c.device_id as device_id_c
        from distinct_identities as a
        left join look_up_existing_user_id as b
        on a.anonymous_id = b.anonymous_id
        left join look_up_existing_email as c
        on a.anonymous_id = c.anonymous_id
      )
      , unique_identities as (
        with coalesce_columns as (
          select
          coalesce(user_id, user_id_b, user_id_c) as user_id
          , coalesce(email, email_b, email_c) as email
          , anonymous_id
          , coalesce(ip_address, ip_address_b, ip_address_c) as ip_address
          , coalesce(device_id, device_id_b, device_id_c) as device_id
          from distinct_identities_filled
        )
        select *
        , "fire_tv" as platform
        , cast(null as string) as phone
        , safe_cast(null as string) as cross_domain_id
        from coalesce_columns
      )
      select distinct * from unique_identities
    )
    , ios_identities as (
      with screens as (
        select
        case
          when safe_cast(user_id as string) = safe_cast(anonymous_id as string) then cast(null as string)
          else safe_cast(user_id as string)
        end as user_id
        , safe_cast(user_email as string) as email
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        , safe_cast(platform as string) as platform
        FROM ios.screens
        group by 1,2,3,4,5,6
      )
      , distinct_identities as (
        select distinct * from screens
      )
      , look_up_existing_user_id as (
        select user_id, email, anonymous_id, ip_address, device_id, platform from distinct_identities where (user_id is not null and email is not null)
      )
      , look_up_existing_email as (
        select user_id, email, anonymous_id, ip_address, device_id, platform from distinct_identities where (user_id is null and email is not null)
      )
      , distinct_identities_filled as (
        select
        a.*
        , b.user_id as user_id_b
        , b.email as email_b
        , b.ip_address as ip_address_b
        , b.device_id as device_id_b
        , b.platform as platform_b
        , c.user_id as user_id_c
        , c.email as email_c
        , c.ip_address as ip_address_c
        , c.device_id as device_id_c
        , c.platform as platform_c
        from distinct_identities as a
        left join look_up_existing_user_id as b
        on a.anonymous_id = b.anonymous_id
        left join look_up_existing_email as c
        on a.anonymous_id = c.anonymous_id
      )
      , unique_identities as (
        with coalesce_columns as (
          select
          coalesce(user_id, user_id_b, user_id_c) as user_id
          , coalesce(email, email_b, email_c) as email
          , anonymous_id
          , coalesce(ip_address, ip_address_b, ip_address_c) as ip_address
          , coalesce(device_id, device_id_b, device_id_c) as device_id
          , coalesce(platform, platform_b, platform_c) as platform
          from distinct_identities_filled
        )
        select *
        , cast(null as string) as phone
        , safe_cast(null as string) as cross_domain_id
        from coalesce_columns
      )
      select distinct * from unique_identities
    )
    , android_identities as (
      with screens as (
        select
        case
          when safe_cast(user_id as string) = safe_cast(anonymous_id as string) then cast(null as string)
          else safe_cast(user_id as string)
        end as user_id
        , safe_cast(user_email as string) as email
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        , safe_cast(platform as string) as platform
        FROM android.screens
        group by 1,2,3,4,5,6
      )
      , distinct_identities as (
        select distinct * from screens
      )
      , look_up_existing_user_id as (
        select user_id, email, anonymous_id, ip_address, device_id, platform from distinct_identities where (user_id is not null and email is not null)
      )
      , look_up_existing_email as (
        select user_id, email, anonymous_id, ip_address, device_id, platform from distinct_identities where (user_id is null and email is not null)
      )
      , distinct_identities_filled as (
        select
        a.*
        , b.user_id as user_id_b
        , b.email as email_b
        , b.ip_address as ip_address_b
        , b.device_id as device_id_b
        , b.platform as platform_b
        , c.user_id as user_id_c
        , c.email as email_c
        , c.ip_address as ip_address_c
        , c.device_id as device_id_c
        , c.platform as platform_c
        from distinct_identities as a
        left join look_up_existing_user_id as b
        on a.anonymous_id = b.anonymous_id
        left join look_up_existing_email as c
        on a.anonymous_id = c.anonymous_id
      )
      , unique_identities as (
        with coalesce_columns as (
          select
          coalesce(user_id, user_id_b, user_id_c) as user_id
          , coalesce(email, email_b, email_c) as email
          , anonymous_id
          , coalesce(ip_address, ip_address_b, ip_address_c) as ip_address
          , coalesce(device_id, device_id_b, device_id_c) as device_id
          , coalesce(platform, platform_b, platform_c) as platform
          from distinct_identities_filled
        )
        select *
        , cast(null as string) as phone
        , safe_cast(null as string) as cross_domain_id
        from coalesce_columns
      )
      select distinct * from unique_identities
    )
    , all_identities as (
      select * from web_identities
      union all
      select * from roku_identities
      union all
      select * from amazon_identities
      union all
      select * from ios_identities
      union all
      select * from android_identities
    )
    select distinct *, row_number() over (order by user_id) as row from all_identities ;;

    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: count_distinct_user_id {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
  }

  dimension: row {
    primary_key: yes
    type: number
    sql: ${TABLE}.row ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: phone {
    type: string
    sql: ${TABLE}.phone ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: cross_domain_id {
    type: string
    sql: ${TABLE}.cross_domain_id ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      phone,
      ip_address,
      anonymous_id,
      cross_domain_id,
      device_id,
      platform
    ]
  }
}
