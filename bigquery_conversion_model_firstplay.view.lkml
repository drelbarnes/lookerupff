view: bigquery_conversion_model_firstplay {
  derived_table: {
    sql:
 WITH
      a AS (
      SELECT
        distinct id AS video_id,
        CASE
          WHEN series LIKE '%Heartland%' THEN 'Heartland'
          WHEN series LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
          ELSE 'Other'
        END AS content
      FROM
        svod_titles.titles_id_mapping),

       web AS (
            SELECT
              user_id,
              'web' AS source,
              timestamp,
              content,
              max(timecode) as duration
            FROM
              javascript.video_playback_started inner join a on video_playback_started.video_id=a.video_id
              group by 1,2,3,4),

            droid AS (
            SELECT
              user_id,
              'android' AS source,
              timestamp,
              content,
              max(timecode) as duration
            FROM
              android.video_playback_started
            INNER JOIN
              a
            ON
              video_playback_started.video_id=a.video_id
              group by 1,2,3,4),

            roku AS (
            SELECT
             user_id,
              'roku' AS source,
              timestamp,
              content,
              max(timecode) as duration
            FROM
              roku.video_playback_started
            INNER JOIN
              a
            ON
              video_playback_started.video_id=a.video_id
              group by 1,2,3,4),

            apple AS (
            SELECT
              user_id,
              'ios' AS source,
              timestamp,
              content,
              max(timecode) as duration
            FROM
              ios.video_playback_started
            INNER JOIN
              a
            ON
              SAFE_CAST(video_playback_started.video_id AS int64)=SAFE_CAST(a.video_id AS int64)
              group by 1,2,3,4),

      b AS (
      SELECT * FROM web
      UNION ALL
      SELECT * FROM droid
      UNION ALL
      SELECT * FROM apple
      UNION ALL
      SELECT * FROM roku),

purchase_event as
(with
b as
(select user_id, min(received_at) as received_at
from http_api.purchase_event
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(received_at) and date(created_at)>'2018-10-31'
group by 1)

select a.user_id, a.platform, created_at
from b inner join http_api.purchase_event as a on a.user_id=b.user_id and a.received_at=b.received_at
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(a.received_at) and date(created_at)>'2018-10-31'),

    c as
    (SELECT
      a.user_id,
      platform,
      created_at,
    --   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
      sum(case when content = 'Heartland' then 1 else 0 end) as watched_heartland,
      sum(case when content = 'Bringing Up Bates'  then 1 else 0 end) as watched_bates,
      sum(case when content = 'Other' then 1 else 0 end) as watched_other,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_heartland_day_1,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_heartland_day_2,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_heartland_day_3,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_heartland_day_4,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_bates_day_1,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_bates_day_2,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_bates_day_3,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_bates_day_4,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_other_day_1,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_other_day_2,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_other_day_3,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_other_day_4
    FROM
       purchase_event as a left join b ON a.user_id=b.user_id
    where date(b.timestamp)>=date(created_at) and date(b.timestamp)<=date_add(date(created_at), interval 14 day)
    group by 1,2,3),

    d as
    (select a.user_id,
           a.platform,
           a.created_at,
           case when watched_heartland is null then 0 else watched_heartland end as watched_heartland,
           case when watched_heartland_day_1 is null then 0 else watched_heartland_day_1 end as watched_heartland_day_1,
           case when watched_heartland_day_2 is null then 0 else watched_heartland_day_2 end as watched_heartland_day_2,
           case when watched_heartland_day_3 is null then 0 else watched_heartland_day_3 end as watched_heartland_day_3,
           case when watched_heartland_day_4 is null then 0 else watched_heartland_day_4 end as watched_heartland_day_4,
           case when watched_bates is null then 0 else watched_bates end as watched_bates,
           case when watched_bates_day_1 is null then 0 else watched_bates_day_1 end as watched_bates_day_1,
           case when watched_bates_day_2 is null then 0 else watched_bates_day_2 end as watched_bates_day_2,
           case when watched_bates_day_3 is null then 0 else watched_bates_day_3 end as watched_bates_day_3,
           case when watched_bates_day_4 is null then 0 else watched_bates_day_4 end as watched_bates_day_4,
           case when watched_other is null then 0 else watched_other end as watched_other,
           case when watched_other_day_1 is null then 0 else watched_other_day_1 end as watched_other_day_1,
           case when watched_other_day_2 is null then 0 else watched_other_day_2 end as watched_other_day_2,
           case when watched_other_day_3 is null then 0 else watched_other_day_3 end as watched_other_day_3,
           case when watched_other_day_4 is null then 0 else watched_other_day_4 end as watched_other_day_4
    from purchase_event as a left join c on a.user_id=c.user_id),

    web_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from javascript.video_playback_started as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    android_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from android.video_playback_started as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    ios_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from ios.video_playback_started  as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    roku_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from roku.video_playback_started  as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    all_days as
    (select * from web_days
    union all
    select * from android_days
    union all
    select * from ios_days
    union all
    select * from roku_days),

    e as
    (select distinct user_id,
                     timestamp
     from all_days),

    f as
    (select user_id,
           count(*) as days_played
    from e
    group by 1),

    g as
    (select d.*,
           coalesce(days_played,0) as days_played
    from d left join f on d.user_id=f.user_id
    where d.user_id<>'0'),

    h as
    (select max(watched_heartland) hl_max, min(watched_heartland) as hl_min,
           max(watched_bates) b_max, min(watched_bates) as b_min,
           max(watched_other) o_max, min(watched_other) as o_min,
           max(watched_heartland_day_1) hl1_max, min(watched_heartland_day_1) as hl1_min,
           max(watched_heartland_day_2) hl2_max, min(watched_heartland_day_2) as hl2_min,
           max(watched_heartland_day_3) hl3_max, min(watched_heartland_day_3) as hl3_min,
           max(watched_heartland_day_4) hl4_max, min(watched_heartland_day_4) as hl4_min,
           max(watched_bates_day_1) b1_max, min(watched_bates_day_1) as b1_min,
           max(watched_bates_day_2) b2_max, min(watched_bates_day_2) as b2_min,
           max(watched_bates_day_3) b3_max, min(watched_bates_day_3) as b3_min,
           max(watched_bates_day_4) b4_max, min(watched_bates_day_4) as b4_min,
           max(watched_other_day_1) o1_max, min(watched_other_day_1) as o1_min,
           max(watched_other_day_2) o2_max, min(watched_other_day_2) as o2_min,
           max(watched_other_day_3) o3_max, min(watched_other_day_3) as o3_min,
           max(watched_other_day_4) o4_max, min(watched_other_day_4) as o4_min,
           max(days_played) dp_max, min(days_played) as dp_min
           from g)


     select user_id,
            platform,
            (watched_heartland - hl_min)/(hl_max - hl_min) as watched_heartland,
            (watched_bates - b_min)/(b_max - b_min) as watched_bates,
            (watched_other - o_min)/(o_max-o_min) as watched_other,
            (watched_heartland_day_1 - hl1_min)/(hl1_max-hl1_min) as watched_heartland_day_1,
            (watched_heartland_day_2 - hl2_min)/(hl2_max-hl2_min) as watched_heartland_day_2,
            (watched_heartland_day_3 - hl3_min)/(hl3_max-hl3_min) as watched_heartland_day_3,
            (watched_heartland_day_4 - hl4_min)/(hl4_max-hl4_min) as watched_heartland_day_4,
            (watched_bates_day_1 - b1_min)/(b1_max-b1_min) as watched_bates_day_1,
            (watched_bates_day_2 - b2_min)/(b2_max-b2_min) as watched_bates_day_2,
            (watched_bates_day_3 - b3_min)/(b3_max-b3_min) as watched_bates_day_3,
            (watched_bates_day_4 - b4_min)/(b4_max-b4_min) as watched_bates_day_4,
            (watched_other_day_1 - o1_min)/(o1_max-o1_min) as watched_other_day_1,
            (watched_other_day_2 - o2_min)/(o2_max-o2_min) as watched_other_day_2,
            (watched_other_day_3 - o3_min)/(o3_max-o3_min) as watched_other_day_3,
            (watched_other_day_4 - o4_min)/(o4_max-o4_min) as watched_other_day_4,
            (days_played - dp_min)/(dp_max-dp_min) as days_played
     from g, h



     ;;}

      dimension: user_id {
        primary_key: yes
        tags: ["user_id"]
        type: string
        sql: ${TABLE}.user_id ;;
      }

      dimension_group: customer_created_at {
        type: time
        timeframes: [
          raw,
          time,
          date,
          week,
          month,
          quarter,
          year
        ]
        sql: ${TABLE}.customer_created_at ;;
      }

      dimension: days_played {
        type: number
        sql: ${TABLE}.days_played ;;
      }

      dimension: platform {
        type: string
        sql: ${TABLE}.platform ;;
      }


      dimension: heartland_play{
        type: number
        sql: ${TABLE}.watched_heartland ;;
      }

      dimension: heartland_play_day_1 {
        type: number
        sql: ${TABLE}.watched_heartland_day_1 ;;
      }

      dimension: heartland_play_day_2 {
        type: number
        sql: ${TABLE}.watched_heartland_day_2 ;;
      }

      dimension: heartland_play_day_3 {
        type: number
        sql: ${TABLE}.watched_heartland_day_3 ;;
      }

  dimension: heartland_play_day_4 {
    type: number
    sql: ${TABLE}.watched_heartland_day_4 ;;
  }

      dimension: bates_play{
        type: number
        sql: ${TABLE}.watched_bates ;;
      }

      dimension: bates_play_day_1 {
        type: number
        sql: ${TABLE}.watched_bates_day_1 ;;
      }

      dimension: bates_play_day_2 {
        type: number
        sql: ${TABLE}.watched_bates_day_2 ;;
      }

      dimension: bates_play_day_3 {
        type: number
        sql: ${TABLE}.watched_bates_day_3 ;;
      }

  dimension: bates_play_day_4 {
    type: number
    sql: ${TABLE}.watched_bates_day_4 ;;
  }

      dimension: other_play {
        type: number
        sql: ${TABLE}.watched_other ;;
      }

      dimension: other_play_day_1 {
        type: number
        sql: ${TABLE}.watched_other_day_1 ;;
      }

      dimension: other_play_day_2 {
        type: number
        sql: ${TABLE}.watched_other_day_2 ;;
      }

      dimension: other_play_day_3 {
        type: number
        sql: ${TABLE}.watched_other_day_3 ;;
      }

  dimension: other_play_day_4 {
    type: number
    sql: ${TABLE}.watched_other_day_4 ;;
  }

    }
