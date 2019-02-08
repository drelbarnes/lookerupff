view: bigquery_churn_model {
  derived_table: {
    sql:
(with a as
(select user_id,
        min(date(status_date)) as conversion_date
 from http_api.purchase_event
 where ((topic='customer.product.renewed' or status='renewed') and date(created_at)>'2018-10-31') or (topic='customer.product.created' and date_diff(date(status_date),date(created_at),day)>14 and date(created_at)>'2018-10-31')
 group by 1),

e as
(select b.user_id,
       created_at,
       conversion_date,
       status_date,
       topic,
       region,
       platform,
       date(status_date) as start_date,
       date_add(date(status_date), interval 30 day) as end_date,
       date_diff(date(status_date),(conversion_date),month) as num,
       case when moptin=true then 1 else 0 end as marketing_optin,
       case when topic in ('customer.product.expired','customer.product.disabled','customer.product.cancelled') then 1 else 0 end as churn_status
from http_api.purchase_event as b inner join a on a.user_id=b.user_id
where conversion_date is not null and topic in ('customer.product.expired','customer.product.disabled','customer.product.cancelled','customer.product.renewed') and (country='United States' or country is null)),


      awl as
      (SELECT user_id,timestamp, 1 as addwatchlist FROM javascript.addwatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as addwatchlist FROM android.addwatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as addwatchlist FROM ios.addwatchlist where user_id is not null),

      f as
      (select e.user_id,
              num,
             case when addwatchlist is null then 0 else 1 end as addwatchlist
      from e left join awl on e.user_id=awl.user_id and date(timestamp) between start_date and end_date),

      awl1 as
      (select user_id,
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
      (select e.user_id,
              num,
             case when error is null then 0 else 1 end as error
      from e left join error on e.user_id=error.user_id and date(timestamp) between start_date and end_date),

      error1 as
      (select user_id,
             num,
             sum(error)-1 as error
      from g
      group by 1,2),

      rwl as
      (SELECT user_id,timestamp, 1 as removewatchlist FROM javascript.removewatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as removewatchlist FROM android.removewatchlist where user_id is not null
        UNION ALL
        SELECT user_id,timestamp, 1 as removewatchlist FROM ios.removewatchlist where user_id is not null),

      i as
      (select e.user_id,
              num,
              case when removewatchlist is null then 0 else 1 end as removewatchlist
      from e left join rwl on e.user_id=rwl.user_id and date(timestamp) between start_date and end_date),

      rwl1 as
      (select user_id,
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
      (select e.user_id,
              num,
              case when view is null then 0 else 1 end as view
      from e left join view on e.user_id=cast(view.user_id as string) and date(timestamp) between start_date and end_date),

      view1 as
      (select user_id,
             num,
             sum(view)-1 as view
      from j
      group by 1,2),

      fp as
      (WITH
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
      (select e.user_id,
              num,
              case when bates_plays is null then 0 else bates_plays end as bates_plays,
              case when heartland_plays is null then 0 else heartland_plays end as heartland_plays,
              case when other_plays is null then 0 else other_plays end as other_plays
      from e left join fp on e.user_id=fp.user_id and date(timestamp) between start_date and end_date),

      fp1 as
      (select user_id,
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
          END AS content,
          current_time as timecode
        FROM
          javascript.timeupdate
        WHERE
          user_id IS NOT NULL),

        android AS (
        SELECT
          user_id,
          'android' AS source,
          timestamp,
          content,
          timecode
        FROM
          android.timeupdate
        INNER JOIN
          a
        ON
          timeupdate.video_id=a.video_id),

        ios AS (
        SELECT
          user_id,
          'ios' AS source,
          timestamp,
          content,
          timecode
        FROM
          ios.timeupdate
        INNER JOIN
          a
        ON
          SAFE_CAST(timeupdate.video_id AS int64)=SAFE_CAST(a.video_id AS int64))


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
      (select e.user_id,
              num,
              case when bates_duration is null then 0 else bates_duration end as bates_duration,
              case when heartland_duration is null then 0 else heartland_duration end as heartland_duration,
              case when other_duration is null then 0 else other_duration end as other_duration
      from e left join duration on e.user_id=duration.user_id and date(timestamp) between start_date and end_date),

      duration1 as
      (select user_id,
             num,
             sum(bates_duration) as bates_duration,
             sum(heartland_duration) as heartland_duration,
             sum(other_duration) as other_duration
      from l
      group by 1,2),

m as
(select e.*,
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
      from e left join awl1 on e.user_id=awl1.user_id and e.num=awl1.num
             left join error1 on e.user_id=error1.user_id and e.num=error1.num
             left join rwl1 on e.user_id=rwl1.user_id and e.num=rwl1.num
             left join view1 on e.user_id=view1.user_id and e.num=view1.num
             left join fp1 on e.user_id=fp1.user_id and e.num=fp1.num
             left join duration1 on e.user_id=duration1.user_id and e.num=duration1.num
      where e.user_id <>'0'),

n as
(select num,
       min(addwatchlist) as awl_min,
       min(error) as error_min,
       min(removewatchlist) as rwl_min,
       min(view) as view_min,
       min(bates_plays) as bp_min,
       min(bates_duration) as bd_min,
       min(heartland_plays) as hlp_min,
       min(heartland_duration) as hld_min,
       min(other_plays) as op_min,
       min(other_duration) as od_min,
       max(addwatchlist) as awl_max,
       max(error) as error_max,
       max(removewatchlist) as rwl_max,
       max(view) as view_max,
       max(bates_plays) as bp_max,
       max(bates_duration) as bd_max,
       max(heartland_plays) as hlp_max,
       max(heartland_duration) as hld_max,
       max(other_plays) as op_max,
       max(other_duration) as od_max
from m
where num<12 and num>=0
group by num)

select m.user_id,
       status_date,
       m.num,
       region as state,
       churn_status,
       end_date,
       platform,
       created_at,
       topic as status,
       marketing_optin,
       (addwatchlist-awl_min)/(awl_max-awl_min) as addwatchlist,
       (error-error_min)/(error_max-error_min) as error,
       (removewatchlist-rwl_min)/(rwl_max-rwl_min) as removewatchlist,
       (view-view_min)/(view_max-view_min) as view,
       (bates_plays-bp_min)/(bp_max-bp_min) as bates_plays,
       (bates_duration-bd_min)/(bd_max-bd_min) as bates_duration,
       (heartland_plays-hlp_min)/(hlp_max-hlp_min) as heartland_plays,
       (heartland_duration-hld_min)/(hld_max-hld_min) as heartland_duration,
       (other_plays-op_min)/(op_max-op_min) as other_plays,
       (other_duration-od_min)/(od_max-od_min) as other_duration
from m left join n on m.num=n.num
where
awl_min<>awl_max and
error_min<>error_max and
rwl_min<>rwl_max and
view_min<>view_max and
bp_min<>bp_max and
bd_min<>bd_max and
hlp_min<>hlp_max and
hld_min<>hld_max and
op_min<>op_max and
od_min<>od_max);;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: marketing_optin {
    type: number
    sql: ${TABLE}.marketing_optin ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: event_created_at {
    type: time
    sql: ${TABLE}.status_date ;;
  }

  dimension_group: customer_created_at {
    type: time
    sql: ${TABLE}.created_at ;;
  }

  dimension: months_since_conversion {
    type: number
    sql: ${TABLE}.months_since_conversion ;;
  }

  dimension: days_since_conversion {
    type: number
    sql: ${TABLE}.days_since_conversion ;;
  }

  dimension: num {
    type: number
    sql: ${TABLE}.num ;;
  }

  dimension: max_num {
    type: number
    sql: ${TABLE}.max_num ;;
  }

  dimension: start_date {
    type: date
    sql: ${TABLE}.start_date ;;
  }

  dimension_group: end_date {
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
    sql: ${TABLE}.end_date ;;
  }

  dimension: churn_status {
    type: number
    sql: ${TABLE}.churn_status ;;
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
      email,
      first_name,
      last_name,
      state,
      status,
      platform,
      event_created_at_time,
      customer_created_at_time,
      months_since_conversion,
      days_since_conversion,
      num,
      churn_status,
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
