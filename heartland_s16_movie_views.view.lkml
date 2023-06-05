view: heartland_s16_movie_views {
    derived_table: {
      sql: with

              vimeo_purchase_event_p0 as
              (
              select
                user_id,
                topic,
                email,
                moptin,
                subscription_status,
                subscription_frequency,
                platform,
                row_number() over (partition by user_id order by timestamp asc) as event_num,
                date(timestamp) as date_stamp,
                subscription_frequency
              from http_api.purchase_event
              where user_id <> '0'
              and regexp_contains(user_id, r'^[0-9]*$')
              order by
                user_id,
                date(timestamp)
              ),

        vimeo_purchase_event_q0 as
        (
        select
        user_id,
        topic,
        email,
        moptin,
        subscription_status,
        subscription_frequency,
        platform,
        row_number() over (partition by user_id order by timestamp desc) as event_num,
        date(timestamp) as date_stamp
        from http_api.purchase_event
        where user_id <> '0'
        and regexp_contains(user_id, r'^[0-9]*$')
        order by
        user_id,
        date(timestamp)
        ),

        distinct_purchase_event as
        (
        select
        distinct user_id,
        topic,
        extract(month from date_stamp) as month,
        extract(year from date_stamp) as year
        from vimeo_purchase_event_p0
        ),

        audience_first_event as
        (
        select
        user_id,
        min(date_stamp) as first_event_date
        from vimeo_purchase_event_p0
        group by user_id
        ),

        audience_last_event as
        (
        select
        user_id,
        email,
        topic,
        moptin,
        platform,
        subscription_frequency,
        date_stamp as last_event
        from vimeo_purchase_event_q0
        where event_num = 1
        ),

        customers_updated_event as
        (
        select
        b.user_id,
        b.moptin,
        b.platform,
        b.topic as vimeo_status,
        b.subscription_frequency,
        b.last_event as last_event_date,
        c.first_event_date
        from audience_last_event as b
        left join audience_first_event as c
        on b.user_id = c.user_id
        ),

        play_data_global as
        (
        select
        *
        from allfirstplay.p0
        where user_id <> '0'
        and regexp_contains(user_id, r'^[0-9]*$')
        and date(timestamp) >= '2022-01-01'
        and date(timestamp) <= current_date()
        ),

        plays_most_granular as
        (
        select
        user_id,
        row_number() over (
        partition by user_id, date(timestamp),
        video_id
        order by date(timestamp)
        ) as min_count,
        timestamp,
        collection,
        type,
        video_id,
        series,
        title,
        source,
        episode,
        email,
        winback
        from play_data_global
        order by
        user_id,
        date(timestamp),
        video_id,
        min_count
        ),

        plays_max_duration as
        (
        select
        user_id,
        video_id,
        date(timestamp) as date,
        max(min_count) as min_count
        from plays_most_granular
        group by 1,2,3
        ),

        plays_less_granular as
        (
        select
        a.*,
        row_number() over (
        partition by a.user_id
        order by a.timestamp
        ) as play_number
        from plays_most_granular as a
        inner join plays_max_duration as b
        on a.user_id = b.user_id
        and a.video_id = b.video_id
        and date(a.timestamp) = b.date
        and a.min_count = b.min_count
        ),

        heartland_audience as
        (
        select distinct
        user_id,
        collection,
        episode
        from plays_less_granular
        where collection = 'Heartland - Season 16'
        and date(timestamp) between '2023-05-30' and '2023-05-31'
        ),

        list_a as
        (
        select
        a.*,
        case when user_id in (select user_id from heartland_audience) then 1 else 0 end as heartland_season_16
        from customers_updated_event as a
        ),

        list_b as
        (
        select
        *,
        date_diff(last_event_date, first_event_date, day) as tenure
        from list_a
        where heartland_season_16 = 1
        ),

        list_c as
        (
        select
        *,
        round(tenure / 30, 0) as tenure2
        from list_b
        ),

        list_d as
        (
        select
        a.*,
        b.topic,
        b.date_stamp
        from list_c as a
        left join vimeo_purchase_event_p0 as b
        on a.user_id = b.user_id
        ),

        list_e as
        (
        select
        user_id,
        case when topic = 'customer.product.renewed' then 1 else 0 end as renew_flag
        from list_d
        ),

        list_f as
        (
        select
        user_id,
        sum(renew_flag) as paid_months
        from list_e
        group by user_id
        ),

        tenure_analysis as
        (
        select
        a.*,
        b.paid_months from list_c as a
        left join list_f as b
        on a.user_id = b.user_id
        ),

        viewing_list_a as
        (
        select
        user_id,
        collection,
        type,
        video_id,
        series,
        title,
        source,
        episode
        from plays_less_granular
        where user_id in
        (select user_id from heartland_audience)
        ),

        viewing_list_movies as
        (
        select
        count(user_id) as views,
        count(distinct user_id) as users,
        title
        from viewing_list_a
        where type = 'movie'
        group by title
        ),

        viewing_list_series as
        (
        select
        count(user_id) as views,
        count(distinct user_id) as users,
        collection
        from viewing_list_a
        where type = 'series'
        group by collection
        ),

        reconciliation as
        (
        select count(distinct user_id) as n, 'views' as ds from viewing_list_a
        union all
        select count(distinct user_id) as n, 'list' as ds from tenure_analysis
        )

        select * from viewing_list_movies order by views

        ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    measure: views {
      type: sum
      sql: ${TABLE}.views ;;
    }

    measure: users {
      type: sum
      sql: ${TABLE}.users ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    set: detail {
      fields: [views, users, title]
    }
  }
