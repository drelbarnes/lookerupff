view: xmas_movies {
    derived_table: {
      sql:
        with

        xmas_titles as (select * from ad_hoc.xmas_titles),

        audience_p0 as (
        select user_id, topic, email, moptin, subscription_status, platform,
        row_number() over (partition by user_id order by timestamp asc) as event_num, case
        when topic like 'customer.product%' then substring(topic, 18, (length(topic) - 1))
        when topic like 'customer%' then substring(topic, 10, (length(topic) - 1))
        else topic end as ctopic,
        date(timestamp) as date_stamp, subscription_frequency
        from http_api.purchase_event
        where user_id is not null
        order by user_id, date(timestamp)
        ),

        audience_p1 as (
        select *
        from audience_p0
        where event_num = 1
        and date_stamp between '2021-11-01' and '2022-12-31'
        ),

        events_p0 as (
        select user_id, ctopic, date_stamp, event_num
        from audience_p0 where user_id in
        (select user_id from audience_p1)
        and date_stamp < '2022-01-16'
        order by user_id, event_num
        ),

        events_p1 as (
        select user_id,
        ltrim(string_agg(concat(' ',ctopic))) as topic_array,
        ltrim(string_agg(concat(' ',date_stamp))) as date_array
        from events_p0
        group by user_id
        ),

        events_p2 as (
        select user_id,
        ltrim(split(topic_array, ',')[safe_ordinal(1)]) as first_topic,
        ltrim(array_reverse(split(topic_array))[safe_offset(0)]) as last_topic,
        ltrim(split(date_array, ',')[safe_ordinal(1)]) as first_date,
        ltrim(array_reverse(split(date_array))[safe_offset(0)]) as last_date,
        topic_array, date_array
        from events_p1
        ),

        events_p3 as (
        select *,
        case when last_topic in ('expired','free_trial_expired','charge_failed','set_cancellation') then 'expired'
        else 'converted' end as status
        from events_p2
        ),

        events_p4 as (
        select user_id, status,
        topic_array, first_topic, last_topic,
        date_array, first_date, last_date,
        date_diff(cast(last_date as date), cast(first_date as date), day) as tenure
        from events_p3
        ),

        titles_p0 as (
        select distinct
        duration_formatted,
        duration_seconds,
        title, video_id
        from php.get_titles
        ),

        titles_p1 as (
        select t1.video_id,
        t1.duration_seconds
        from titles_p0 as t1
        inner join (select distinct video_id,
        max(duration_seconds) as duration_max
        from titles_p0 group by 1) as t2
        on t1.video_id = t2.video_id
        and t1.duration_seconds = t2.duration_max
        ),

        titles_p2 as (
        select video_id, num_dup
        from (select video_id, count(video_id) as num_dup
        from titles_p1 group by video_id having num_dup > 1)
        group by 1,2
        ),

        titles_p3 as (select distinct * from titles_p1),

        plays_p0 as (
        select user_id, timestamp,
        collection, video_id, title,
        source, winback, row_number() over
        (partition by user_id, video_id order by date(timestamp)) as mins
        from allfirstplay.p0 where title in
        (select name from ad_hoc.xmas_titles)
        and date(timestamp) between '2021-11-01' and '2022-01-16'
        and regexp_contains(user_id, r'^[0-9]*$')
        and user_id is not null
        and user_id <> '0'
        ),

        plays_p1 as (
        select user_id, date(timestamp) as date_stamp,
        collection, video_id, title, source, winback,
        max(mins) as mins_watched
        from plays_p0
        group by 1,2,3,4,5,6,7
        ),

        plays_p2 as (
        select t1.*,
        round(t2.duration_seconds / 60, 0) as mins_duration
        from plays_p1 as t1
        inner join titles_p3 as t2
        on t1.video_id = t2.video_id
        ),

        plays_p3 as (
        select *, case
        when mins_watched > mins_duration then mins_duration
        else mins_watched end as mins_viewed
        from plays_p2
        ),

        plays_p4 as (
        select * except
        (mins_watched),
        round((mins_viewed / mins_duration * 100), 1) as completion_rate
        from plays_p3
        ),

        analysis_p0 as (
        select t1.*, t2.* except
        (user_id, topic_array, date_array)
        from plays_p4 as t1
        left join events_p4 as t2
        on t1.user_id = t2.user_id
        where status is not null
        ),

        analysis_p1 as
        (
        select title, status,
        count(user_id) as number_plays,
        round(avg(completion_rate), 1) as avg_completion
        from analysis_p0
        group by title, status
        ),

        analysis_p2 as (
        select *,
        (select count(distinct user_id) from events_p4 where status = 'expired') as number_expired,
        (select count(distinct user_id) from events_p4 where status = 'converted') as number_converted
        from analysis_p1
        ),

        analysis_p3 as (
        select title, status, number_plays, case
        when status = 'converted' then round(number_plays / number_converted * 100, 2)
        when status = 'expired' then round(number_plays / number_expired * 100, 2)
        else 0.00 end as percent_viewed,
        avg_completion, number_expired, number_converted
        from analysis_p2
        ),

        missing_p0 as (
        select name, item_id
        from xmas_titles
        where name not in
        (select title from analysis_p1)
        )

        select * from analysis_p3 order by title

        -- select count(video_id) as n, count(distinct video_id) as m from titles_p3
        -- select count(distinct user_id) as n, count(user_id) as m, status from events_p4 group by status -- 33,801 converted, 11,983 expired
        -- select count(user_id) as n, count(distinct user_id) as m from audience_p1 -- 45,784
        ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: status {
      type: string
      sql: ${TABLE}.status ;;
    }

    measure: number_plays {
      type: sum
      sql: ${TABLE}.number_plays ;;
    }

    dimension: percent_viewed {
      type: number
      sql: ${TABLE}.percent_viewed ;;
    }

    dimension: avg_completion {
      type: number
      sql: ${TABLE}.avg_completion ;;
    }

    dimension: number_expired {
      type: number
      sql: ${TABLE}.number_expired ;;
    }

    dimension: number_converted {
      type: number
      sql: ${TABLE}.number_converted ;;
    }

    set: detail {
      fields: [
        title,
        status,
        number_plays,
        percent_viewed,
        avg_completion,
        number_expired,
        number_converted
      ]
    }
  }
