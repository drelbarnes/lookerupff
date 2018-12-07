view: bigquery_personas {
  derived_table: {
    sql:
with a as
      (SELECT -1+ROW_NUMBER() OVER() AS num
      FROM UNNEST((SELECT SPLIT(FORMAT("%600s", ""),'') AS h FROM (SELECT NULL))) AS pos
      ORDER BY num),

      b as
      (select *,
             case when status in ("enabled") then (date_diff(current_date, date_add(date(customer_created_at), interval 14 day), month))
                  when status in ("cancelled","disabled","refunded","expired") and (date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), month))>=0
                       then (date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), month))
                  when status in ("cancelled","disabled","refunded","expired") and (date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), month))<0 then 0
             end as months_since_conversion,
             case when status in ("enabled") then date_diff(current_date, date_add(date(customer_created_at), interval 14 day), day)
                  when status in ("cancelled","disabled","refunded","expired") then date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), day)
             end as days_since_conversion
      from customers.subscribers
      where status is not null or status not in ("free_trial","paused") and platform<>'roku'
      order by months_since_conversion desc),

      c as
      (select customer_id,
             max(num) as max_num
      from b, a
      where months_since_conversion>=num
      group by customer_id),

      d as
      (select b.*,
              num
      from b, a
      where months_since_conversion>=num),

      e as (
      select d.*,
             case when num=0 then date(customer_created_at) else
             date_add(date_add(date(customer_created_at), interval 14 day),interval num*30 day) end as start_date,
             case when status="enabled" or num<max_num then date_add(date_add(date(customer_created_at), interval 14 day),interval (num+1)*30 day)
                  when status<>"enabled" and num=max_num then date(event_created_at) end as end_date,
             case when status="enabled" or num<max_num then 0
                  when status<>"enabled" and num=max_num then 1 end as churn_status
      from d inner join c on d.customer_id=c.customer_id
      where platform<>'roku'),

      awl as
      (SELECT user_id,timestamp, 1 as addwatchlist FROM javascript.addwatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as addwatchlist FROM android.addwatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as addwatchlist FROM ios.addwatchlist where user_id is not null),

      f as
      (select customer_id,
              num,
             case when addwatchlist is null then 0 else 1 end as addwatchlist
      from e left join awl on customer_id=safe_cast(user_id as int64) and date(timestamp) between start_date and end_date),

      awl1 as
      (select customer_id,
             num,
             sum(addwatchlist) as addwatchlist
      from f
      group by 1,2),

      error as
      (SELECT user_id,timestamp, 1 as error FROM javascript.error where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as error FROM android.error where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as error FROM ios.error where user_id is not null),

      g as
      (select customer_id,
              num,
             case when error is null then 0 else 1 end as error
      from e left join error on customer_id=safe_cast(user_id as int64) and date(timestamp) between start_date and end_date),

      error1 as
      (select customer_id,
             num,
             sum(error) as error
      from g
      group by 1,2),

      rwl as
      (SELECT user_id,timestamp, 1 as removewatchlist FROM javascript.removewatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as removewatchlist FROM android.removewatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as removewatchlist FROM ios.removewatchlist where user_id is not null),

      i as
      (select customer_id,
              num,
              case when removewatchlist is null then 0 else 1 end as removewatchlist
      from e left join rwl on customer_id=safe_cast(user_id as int64) and date(timestamp) between start_date and end_date),

      rwl1 as
      (select customer_id,
             num,
             sum(removewatchlist) as removewatchlist
      from i
      group by 1,2),

      view as
      (SELECT safe_cast(user_id as int64) as user_id,timestamp, 1 as view FROM javascript.pages where user_id is not null
        UNION ALL
        SELECT safe_cast(user_id as int64) as user_id,timestamp, 1 as view FROM android.view where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as view FROM ios.view where user_id is not null),

      j as
      (select customer_id,
              num,
              case when view is null then 0 else 1 end as view
      from e left join view on customer_id=safe_cast(user_id as int64) and date(timestamp) between start_date and end_date),

      view1 as
      (select customer_id,
             num,
             sum(view) as view
      from j
      group by 1,2),

      fp as
      (WITH
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
              SAFE_CAST(firstplay.video_id AS int64)=SAFE_CAST(a.video_id AS int64))


            SELECT user_id,
                   (timestamp) as timestamp,
                   (case when content="Other" then 1 else 0 end) as other_plays,
                   (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
                   (case when content="Heartland" then 1 else 0 end) as heartland_plays
            FROM web
            UNION ALL
            SELECT user_id,
                   (timestamp) as timestamp,
                   (case when content="Other" then 1 else 0 end) as other_plays,
                   (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
                   (case when content="Heartland" then 1 else 0 end) as heartland_plays
            FROM droid
            UNION ALL
            SELECT user_id,
                   (timestamp) as timestamp,
                   (case when content="Other" then 1 else 0 end) as other_plays,
                   (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
                   (case when content="Heartland" then 1 else 0 end) as heartland_plays
            FROM apple),

      k as
      (select customer_id,
              num,
              case when bates_plays is null then 0 else bates_plays end as bates_plays,
              case when heartland_plays is null then 0 else heartland_plays end as heartland_plays,
              case when other_plays is null then 0 else other_plays end as other_plays
      from e left join fp on customer_id=safe_cast(user_id as int64) and date(timestamp) between start_date and end_date),

      fp1 as
      (select customer_id,
             num,
             sum(bates_plays) as bates_plays,
             sum(heartland_plays) as heartland_plays,
             sum(other_plays) as other_plays
      from k
      group by 1,2),

      duration as
      (WITH
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

        web0 as
        (select user_id,
                date(sent_at) as timestamp,
                title,
                max(a.current_time) as timecode
         from javascript.timeupdate as a
         where user_id <> '0' and user_id is not null
         group by 1,2,3),

         android0 as
        (select user_id,
                date(sent_at) as timestamp,
                video_id,
                max(timecode) as timecode
         from android.timeupdate
         where user_id <> '0' and user_id is not null
         group by 1,2,3),

         ios0 as
        (select user_id,
                date(sent_at) as timestamp,
                video_id,
                max(timecode) as timecode
         from ios.timeupdate
         where user_id <> '0' and user_id is not null
         group by 1,2,3),

        web AS (
        SELECT
          user_id,
          'web' AS source,
          timestamp,
          CASE
            WHEN upper(title) LIKE '%HEARTLAND%' THEN 'Heartland'
            WHEN upper(title) LIKE '%BRINGING UP BATES%' THEN 'Bringing Up Bates'
            ELSE 'Other'
          END AS content,
          timecode
        FROM
          web0),

        android AS (
        SELECT
          user_id,
          'android' AS source,
          timestamp,
          content,
          timecode
        FROM
          android0
        INNER JOIN
          a
        ON
          android0.video_id=a.video_id),

        ios AS (
        SELECT
          user_id,
          'ios' AS source,
          timestamp,
          content,
          timecode
        FROM
          ios0
        INNER JOIN
          a
        ON
          SAFE_CAST(ios0.video_id AS int64)=SAFE_CAST(a.video_id AS int64))


        SELECT user_id,
               source,
               timestamp,
               case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
               case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
               case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
        FROM web
        UNION ALL
        SELECT user_id,
               source,
               timestamp,
               case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
               case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
               case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
        FROM android
        UNION ALL
        SELECT user_id,
               source,
               timestamp,
               case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
               case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
               case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
         FROM ios),

      l as
      (select customer_id,
              num,
              case when bates_duration is null then 0 else bates_duration end as bates_duration,
              case when heartland_duration is null then 0 else heartland_duration end as heartland_duration,
              case when other_duration is null then 0 else other_duration end as other_duration
      from e left join duration on customer_id=safe_cast(user_id as int64) and (timestamp) between start_date and end_date),

      duration1 as
      (select customer_id,
             num,
             sum(bates_duration) as bates_duration,
             sum(heartland_duration) as heartland_duration,
             sum(other_duration) as other_duration
      from l
      group by 1,2)


(select      e.customer_id,
             days_since_conversion,
             sum(case when months_since_conversion=0 then addwatchlist else addwatchlist*((e.num)/months_since_conversion) end) as addwatchlist,
             sum(case when months_since_conversion=0 then error else error*((e.num)/months_since_conversion) end) as error,
             sum(case when months_since_conversion=0 then removewatchlist else removewatchlist*((e.num)/months_since_conversion) end) as removewatchlist,
             sum(case when months_since_conversion=0 then view else view*((e.num)/months_since_conversion) end) as view,
             sum(case when months_since_conversion=0 then bates_plays else bates_plays*((e.num)/months_since_conversion) end) as bates_plays,
             sum(case when months_since_conversion=0 then bates_duration else bates_duration*((e.num)/months_since_conversion) end) as bates_duration,
             sum(case when months_since_conversion=0 then heartland_plays else heartland_plays*((e.num)/months_since_conversion) end) as heartland_plays,
             sum(case when months_since_conversion=0 then heartland_duration else heartland_duration*((e.num)/months_since_conversion) end) as heartland_duration,
             sum(case when months_since_conversion=0 then other_plays else other_plays*((e.num)/months_since_conversion) end) as other_plays,
             sum(case when months_since_conversion=0 then other_duration else other_duration*((e.num)/months_since_conversion) end) as other_duration
      from e left join awl1 on e.customer_id=awl1.customer_id and e.num=awl1.num
             left join error1 on e.customer_id=error1.customer_id and e.num=error1.num
             left join rwl1 on e.customer_id=rwl1.customer_id and e.num=rwl1.num
             left join view1 on e.customer_id=view1.customer_id and e.num=view1.num
             left join fp1 on e.customer_id=fp1.customer_id and e.num=fp1.num
             left join duration1 on e.customer_id=duration1.customer_id and e.num=duration1.num
      where e.customer_id <>0 and status='enabled'
      group by 1,2)
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: months_since_conversion {
    type: number
    sql: ${TABLE}.months_since_conversion ;;
  }

  dimension: tenure {
    type: number
    sql: ${TABLE}.tenure ;;
  }

  dimension: addwatchlist {
    type: number
    sql: ${TABLE}.addwatchlist ;;
  }

  dimension: error {
    type: number
    sql: ${TABLE}.error ;;
  }

  dimension: removewatchlist {
    type: number
    sql: ${TABLE}.removewatchlist ;;
  }

  dimension: view {
    type: number
    sql: ${TABLE}.view ;;
  }

  dimension: bates_plays {
    type: number
    sql: ${TABLE}.bates_plays ;;
  }

  dimension: bates_duration {
    type: number
    sql: ${TABLE}.bates_duration ;;
  }

  dimension: heartland_plays {
    type: number
    sql: ${TABLE}.heartland_plays ;;
  }

  dimension: heartland_duration {
    type: number
    sql: ${TABLE}.heartland_duration ;;
  }

  dimension: other_plays {
    type: number
    sql: ${TABLE}.other_plays ;;
  }

  dimension: other_duration {
    type: number
    sql: ${TABLE}.other_duration ;;
  }

  set: detail {
    fields: [
      customer_id,
      state,
      status,
      frequency,
      platform,
      months_since_conversion,
      tenure,
      addwatchlist,
      error,
      removewatchlist,
      view,
      bates_plays,
      bates_duration,
      heartland_plays,
      heartland_duration,
      other_plays,
      other_duration
    ]
  }
}
