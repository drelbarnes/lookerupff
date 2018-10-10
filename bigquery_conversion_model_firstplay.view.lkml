view: bigquery_conversion_model_firstplay {
  derived_table: {
    sql:
    WITH
      a AS (
      SELECT
        id AS video_id,
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
        CASE
          WHEN upper(title) LIKE '%HEARTLAND%' THEN 'Heartland'
          WHEN upper(title) LIKE '%BRINGING UP BATES%' THEN 'Bringing Up Bates'
          ELSE 'Other'
        END AS content
      FROM
        javascript.firstplay
      WHERE
        user_id IS NOT NULL),

      droid AS (
      SELECT
        user_id,
        'android' AS source,
        timestamp,
        content
      FROM
        android.firstplay
      INNER JOIN
        a
      ON
        firstplay.video_id=a.video_id),

      apple AS (
      SELECT
        user_id,
        'ios' AS source,
        timestamp,
        content
      FROM
        ios.firstplay
      INNER JOIN
        a
      ON
        SAFE_CAST(firstplay.video_id AS int64)=SAFE_CAST(a.video_id AS int64)),

      b AS (
      SELECT * FROM web
      UNION ALL
      SELECT * FROM droid
      UNION ALL
      SELECT * FROM apple),

    c as
    (SELECT
      user_id,
      platform,
      frequency,
      case when campaign is not null then campaign else 'unavailable' end as campaign,
      customer_created_at,
    --   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
      sum(case when content = 'Heartland' then 1 else 0 end) as watched_heartland,
      sum(case when content = 'Bringing Up Bates'  then 1 else 0 end) as watched_bates,
      sum(case when content = 'Other' then 1 else 0 end) as watched_other,
      sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)<4 then 1 else 0 end) as watched_heartland_day_1,
      sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then 1 else 0 end) as watched_heartland_day_2,
      sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then 1 else 0 end) as watched_heartland_day_3,
      sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then 1 else 0 end) as watched_heartland_day_4,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)<4 then 1 else 0 end) as watched_bates_day_1,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then 1 else 0 end) as watched_bates_day_2,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then 1 else 0 end) as watched_bates_day_3,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then 1 else 0 end) as watched_bates_day_4,
      sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)<4 then 1 else 0 end) as watched_other_day_1,
      sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then 1 else 0 end) as watched_other_day_2,
      sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then 1 else 0 end) as watched_other_day_3,
      sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then 1 else 0 end) as watched_other_day_4
    FROM
      b LEFT JOIN customers.subscribers ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64)
    where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)
    group by 1,2,3,4,5
    order by user_id),

    d as
    (select customer_id as user_id,
           a.platform,
           a.frequency,
           case when a.campaign is not null then a.campaign else 'unavailable' end as campaign,
           a.customer_created_at,
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
    from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64)),

    web_days as
    (select distinct user_id,
                    date(timestamp) as timestamp
    from javascript.firstplay right join customers.subscribers on safe_cast(user_id as int64)=customer_id
    where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)),

    android_days as
    (select distinct user_id,
                    date(timestamp) as timestamp
    from android.firstplay right join customers.subscribers on safe_cast(user_id as int64)=customer_id
    where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)),

    ios_days as
    (select distinct user_id,
                    date(timestamp) as timestamp
    from ios.firstplay right join customers.subscribers on safe_cast(user_id as int64)=customer_id
    where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)),

    all_days as
    (select * from web_days
    union all
    select * from android_days
    union all
    select * from ios_days),

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
    from d left join f on d.user_id=safe_cast(f.user_id as int64)),

    h as
    (select avg(watched_heartland) hl_avg, stddev(watched_heartland) as hl_std,
           avg(watched_bates) b_avg, stddev(watched_bates) as b_std,
           avg(watched_other) o_avg, stddev(watched_other) as o_std,
           avg(watched_heartland_day_1) hl1_avg, stddev(watched_heartland_day_1) as hl1_std,
           avg(watched_heartland_day_2) hl2_avg, stddev(watched_heartland_day_2) as hl2_std,
           avg(watched_heartland_day_3) hl3_avg, stddev(watched_heartland_day_3) as hl3_std,
           avg(watched_heartland_day_4) hl4_avg, stddev(watched_heartland_day_4) as hl4_std,
           avg(watched_bates_day_1) b1_avg, stddev(watched_bates_day_1) as b1_std,
           avg(watched_bates_day_2) b2_avg, stddev(watched_bates_day_2) as b2_std,
           avg(watched_bates_day_3) b3_avg, stddev(watched_bates_day_3) as b3_std,
           avg(watched_bates_day_4) b4_avg, stddev(watched_bates_day_4) as b4_std,
           avg(watched_other_day_1) o1_avg, stddev(watched_other_day_1) as o1_std,
           avg(watched_other_day_2) o2_avg, stddev(watched_other_day_2) as o2_std,
           avg(watched_other_day_3) o3_avg, stddev(watched_other_day_3) as o3_std,
           avg(watched_other_day_4) o4_avg, stddev(watched_other_day_4) as o4_std,
           avg(days_played) dp_avg, stddev(days_played) as dp_std
           from g)

     select user_id,
            platform,
            frequency,
            campaign,
            (watched_heartland - hl_avg)/hl_std as watched_heartland,
            (watched_bates - b_avg)/b_std as watched_bates,
            (watched_other - o_avg)/o_std as watched_other,
            (watched_heartland_day_1 - hl1_avg)/hl1_std as watched_heartland_day_1,
            (watched_heartland_day_2 - hl2_avg)/hl2_std as watched_heartland_day_2,
            (watched_heartland_day_3 - hl3_avg)/hl3_std as watched_heartland_day_3,
            (watched_heartland_day_4 - hl4_avg)/hl4_std as watched_heartland_day_4,
            (watched_bates_day_1 - b1_avg)/b1_std as watched_bates_day_1,
            (watched_bates_day_2 - b2_avg)/b2_std as watched_bates_day_2,
            (watched_bates_day_3 - b3_avg)/b3_std as watched_bates_day_3,
            (watched_bates_day_4 - b4_avg)/b4_std as watched_bates_day_4,
            (watched_other_day_1 - o1_avg)/o1_std as watched_other_day_1,
            (watched_other_day_2 - o2_avg)/o2_std as watched_other_day_2,
            (watched_other_day_3 - o3_avg)/o3_std as watched_other_day_3,
            (watched_other_day_4 - o4_avg)/o4_std as watched_other_day_4,
            (days_played - dp_avg)/dp_std as days_played
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

      dimension: frequency {
        type: string
        sql: ${TABLE}.frequency ;;
      }

      dimension: campaign {
        type: string
        sql: ${TABLE}.campaign ;;
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
