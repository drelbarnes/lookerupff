view: heartland_analysis {
  derived_table: {
    sql:
      with

      vimeo_purchase_event_first as
      (
      select user_id, topic, email, moptin, subscription_status, platform,
      row_number() over (partition by user_id order by timestamp asc) as event_num,
      date(timestamp) as date_stamp, subscription_frequency
      from http_api.purchase_event
      order by user_id, date(timestamp)
      ),

      vimeo_purchase_event_last as
      (
      select user_id, topic, email, moptin, subscription_status, platform,
      row_number() over (partition by user_id order by timestamp desc) as event_num,
      date(timestamp) as date_stamp, subscription_frequency
      from http_api.purchase_event
      order by user_id, date(timestamp)
      ),

      adjacent_event_p1 as
      (
      select user_id,
      date_stamp as cdate,
      lead(date_stamp) over (partition by user_id order by event_num) as fdate,
      topic as current_topic,
      lead(topic) over (partition by user_id order by event_num) as future_topic,
      subscription_status as cstatus,
      lead(subscription_status) over (partition by user_id order by event_num) as fstatus,
      subscription_status, email, platform, event_num, subscription_frequency
      from vimeo_purchase_event_first
      ),

      adjacent_event_p2 as
      (
      select user_id, cdate, fdate, current_topic, future_topic,
      case
      when current_topic like 'customer.product%' then substring(current_topic, 18, (length(current_topic) - 1))
      when current_topic like 'customer%' then substring(current_topic, 10, (length(current_topic) - 1))
      else current_topic end as ctopic,
      case
      when future_topic like 'customer.product%' then substring(future_topic, 18, (length(future_topic) - 1))
      when future_topic like 'customer%' then substring(future_topic, 10, (length(future_topic) - 1))
      else future_topic end as ftopic,
      cstatus, fstatus, email, platform, event_num,
      subscription_frequency
      from adjacent_event_p1
      ),

      adjacent_event as
      (
      select user_id, cdate, fdate,
      date_diff(fdate, cdate, day) as days_between,
      ctopic, ftopic, concat(ctopic, ' || ', ftopic) as topic_cnv,
      cstatus, fstatus, concat(cstatus, ' || ', fstatus) as status_cnv,
      email, platform, event_num, subscription_frequency
      from adjacent_event_p2
      ),

      old_customers_lookup as
      (
      select * from adjacent_event
      where ctopic = 'customer.product.cancelled'
      and cdate < '2022-03-15'
      order by user_id, cdate
      ),

      winback_customers_weak as
      (
      select * from adjacent_event
      where cstatus in ('enabled','approved','free_trial')
      and user_id in (select user_id from old_customers_lookup)
      and cdate > '2022-03-15'
      order by user_id, cdate
      ),

      new_customers as
      (
      select *, 'new-customers' as type
      from adjacent_event
      where cstatus in ('enabled','approved','free_trial')
      and event_num = 1 and cdate between '2022-03-15' and '2022-06-05'
      order by user_id, cdate
      ),

      winback_voluntary as
      (
      select *, 'winback-voluntary' as type
      from adjacent_event
      where fdate between '2022-03-15' and '2022-06-05'
      and topic_cnv in (
      'cancelled || created',
      'cancelled || free_trial_created',
      'cancelled || renewed',
      'cancelled || updated')
      ),

      winback_voluntary_duplicates as
      (
      select user_id, num_dup, count(user_id) as n from (
      select count(user_id) as num_dup, user_id
      from winback_voluntary
      group by user_id
      having num_dup > 1)
      group by user_id, num_dup
      order by num_dup
      ),

      winback_voluntary_duplicates_list as
      (
      select * from winback_voluntary
      where user_id in (select user_id
      from winback_voluntary_duplicates)
      ),

      winback_voluntary_unique_p1 as
      (
      select user_id,
      min(event_num) as first_event
      from winback_voluntary
      group by user_id
      ),

      reacquisitions as
      (
      select a.*
      from winback_voluntary as a
      join winback_voluntary_unique_p1 as b
      on a.user_id = b.user_id
      and a.event_num = b.first_event
      where a.user_id not in (select user_id from new_customers)
      ),

      customer_universe as
      (
      select * from new_customers union all
      select * from reacquisitions
      ),

      customer_universe_duplicates as
      (
      select user_id, num_dup, count(user_id) as n from (
      select count(user_id) as num_dup, user_id
      from customer_universe
      group by user_id
      having num_dup > 1)
      group by user_id, num_dup
      order by num_dup
      ),

      customer_universe_duplicates_list as
      (
      select * from customer_universe
      where user_id in (select user_id
      from customer_universe_duplicates)
      ),

      winback_involuntary as
      (
      select *, 'winback-involuntary' as type
      from adjacent_event
      where fdate between '2022-03-15' and '2022-06-05'
      and topic_cnv in (
      'charge_failed || created',
      'charge_failed || free_trial_converted',
      'charge_failed || renewed',
      'charge_failed || updated')
      ),

      winback_involuntary2 as
      (
      select * from adjacent_event
      where fdate > '2022-03-15'
      and topic_cnv in (
      'expired || created',
      'expired || free_trial_converted',
      'expired || renewed',
      'expired || updated')
      ),

      play_data_global as
      (
      select * from ad_hoc.allfirstplay
      where user_id <> '0'
      and regexp_contains(user_id, r'^[0-9]*$')
      and date(timestamp) >= '2020-03-15'
      and date(timestamp) <= '2022-09-15'
      ),

      plays_most_granular as
      (
      select user_id,
      row_number() over (
      partition by user_id,
      date(timestamp), video_id
      order by date(timestamp)
      ) as min_count,
      timestamp, collection, type, video_id, series,
      title, source, episode, email, winback
      from play_data_global
      order by user_id, date(timestamp), video_id, min_count
      ),

      plays_max_duration as
      (
      select user_id, video_id,
      date(timestamp) as date,
      max(min_count) as min_count
      from plays_most_granular
      group by 1,2,3
      ),

      plays_less_granular as
      (
      select a.*, row_number() over (
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

      heartland_users_s15 as
      (
      select distinct user_id
      from plays_less_granular
      where collection = 'Heartland - Season 15'
      ),

      customer_universe_heartland as
      (
      select user_id, cdate, fdate, days_between,
      topic_cnv, status_cnv, email, platform, event_num,
      type, subscription_frequency
      from customer_universe
      where user_id in (
      select user_id
      from heartland_users_s15)
      ),

      churn_date_p1 as
      (
      select user_id, topic as last_topic,
      subscription_status as last_status,
      platform, date_stamp as last_event_dt,
      case when subscription_status in ('cancelled', 'expired', 'disabled') then 'churn' else 'active' end as status
      from vimeo_purchase_event_last
      where event_num = 1
      and date_stamp > '2022-03-15'
      ),

      churn_date_p2 as
      (
      select a.*, b.last_topic, b.last_status, b.last_event_dt,
      b.status, date_diff(b.last_event_dt, a.fdate, day) as tenure
      from customer_universe_heartland as a
      join churn_date_p1 as b
      on a.user_id = b.user_id
      ),

      churn_date_p3 as
      (
      select *,
      case
      when status = 'churn' and tenure between 0 and 30 then 'A. 0-30'
      when status = 'churn' and tenure between 30 and 60 then 'B. 30-60'
      when status = 'churn' and tenure between 60 and 90 then 'C. 60-90'
      when status = 'churn' and tenure between 90 and 120 then 'D. 90-120'
      when status = 'churn' and tenure between 120 and 150 then 'E. 120-150'
      when status = 'churn' and tenure between 150 and 180 then 'F. 150-180'
      when status = 'churn' and tenure > 180 then 'G. 180+'
      when status = 'active' then 'H. Active'
      else 'missing' end as tenure_flag
      from churn_date_p2
      ),

      churn_date as
      (
      select user_id, fdate as first_event_dt, last_event_dt,
      tenure, tenure_flag, platform, type as cust_type, status,
      last_status, last_topic, email, topic_cnv, status_cnv,
      days_between, subscription_frequency
      from churn_date_p3
      ),

      heartland_bingers_p1 as
      (
      select *,
      case when collection = 'Heartland - Season 15' then 1 else 0 end as s15_flag
      from plays_less_granular
      where date(timestamp) > '2022-03-15'
      ),

      heartland_bingers_p2 as
      (
      select user_id,
      sum(s15_flag) over (partition by user_id) as number_heartland_s15_views
      from heartland_bingers_p1
      ),

      heartland_bingers as
      (
      select distinct * from heartland_bingers_p2
      ),

      churn_analysis_bingers_p1 as
      (
      select a.*, b.number_heartland_s15_views
      from churn_date as a
      join heartland_bingers as b
      on a.user_id = b.user_id
      ),

      churn_analysis_bingers_p2 as
      (
      select *,
      case when cust_type = 'new-customers' and (status_cnv like '%free_trial%' or last_topic like '%free_trial%') then 'free-trial'
      else 'non-free-trial' end as free_trial
      from churn_analysis_bingers_p1
      ),

      churn_analysis_bingers as
      (
      select *,
      case when number_heartland_s15_views > 9 then 'binger' else 'non-binger' end as binger,
      case when cust_type = 'new-customers' then round((tenure - 14) * 0.2, 2)
      else round(tenure * 0.2, 2)
      end as ltv_estimate
      from churn_analysis_bingers_p2
      where first_event_dt <> last_event_dt
      )

      select * from churn_analysis_bingers
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: ltv_estimate {
    type: sum
    value_format: "$#,##0.00"
    sql: ${TABLE}.ltv_estimate ;;
  }

  measure: number_heartland_s15_views {
    type: average
    sql: ${TABLE}.number_heartland_s15_views ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: first_event_dt {
    type: date
    datatype: date
    sql: ${TABLE}.first_event_dt ;;
  }

  dimension: last_event_dt {
    type: date
    datatype: date
    sql: ${TABLE}.last_event_dt ;;
  }

  dimension: tenure {
    type: number
    sql: ${TABLE}.tenure ;;
  }

  dimension: tenure_flag {
    type: string
    sql: ${TABLE}.tenure_flag ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: cust_type {
    type: string
    sql: ${TABLE}.cust_type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: last_status {
    type: string
    sql: ${TABLE}.last_status ;;
  }

  dimension: last_topic {
    type: string
    sql: ${TABLE}.last_topic ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: topic_cnv {
    type: string
    sql: ${TABLE}.topic_cnv ;;
  }

  dimension: status_cnv {
    type: string
    sql: ${TABLE}.status_cnv ;;
  }

  dimension: days_between {
    type: number
    sql: ${TABLE}.days_between ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: free_trial {
    type: string
    sql: ${TABLE}.free_trial ;;
  }

  dimension: binger {
    type: string
    sql: ${TABLE}.binger ;;
  }

  set: detail {
    fields: [
      user_id,
      first_event_dt,
      last_event_dt,
      tenure,
      tenure_flag,
      platform,
      cust_type,
      status,
      last_status,
      last_topic,
      email,
      topic_cnv,
      status_cnv,
      days_between,
      subscription_frequency,
      number_heartland_s15_views,
      free_trial,
      binger,
      ltv_estimate
    ]
  }
}
