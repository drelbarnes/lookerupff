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
      , distinct_identities as (
      select distinct * from union_all
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
      select
      safe_cast(user_id as string) as user_id
      , safe_cast(user_email as string) as email
      , cast(null as string) as phone
      , cast(null as string) as ip_address
      , safe_cast(anonymous_id as string) as anonymous_id
      , cast(null as string) as cross_domain_id
      , safe_cast(device_id as string) as device_id
      , safe_cast(platform as string) as platform
      FROM roku.sign_in_complete
      group by 1,2,3,4,5,6,7,8
    )
    , amazon_identities as (
      select
      safe_cast(user_id as string) as user_id
      , safe_cast(user_email as string) as email
      , cast(null as string) as phone
      , safe_cast(context_ip as string) as ip_address
      , safe_cast(anonymous_id as string) as anonymous_id
      , cast(null as string) as cross_domain_id
      , safe_cast(device_id as string) as device_id
      , safe_cast(platform as string) as platform
      FROM amazon_fire_tv.sign_in_complete
      group by 1,2,3,4,5,6,7,8
    )
    , ios_identities as (
      select
      safe_cast(user_id as string) as user_id
      , safe_cast(user_email as string) as email
      , cast(null as string) as phone
      , safe_cast(context_ip as string) as ip_address
      , safe_cast(anonymous_id as string) as anonymous_id
      , cast(null as string) as cross_domain_id
      , safe_cast(device_id as string) as device_id
      , safe_cast(platform as string) as platform
      FROM ios.sign_in_complete
      group by 1,2,3,4,5,6,7,8
    )
    , android_identities as (
      select
      safe_cast(user_id as string) as user_id
      , safe_cast(user_email as string) as email
      , cast(null as string) as phone
      , safe_cast(context_ip as string) as ip_address
      , safe_cast(anonymous_id as string) as anonymous_id
      , cast(null as string) as cross_domain_id
      , safe_cast(device_id as string) as device_id
      , safe_cast(platform as string) as platform
      FROM android.sign_in_complete
      group by 1,2,3,4,5,6,7,8
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

    persist_for: "12 hours"
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
