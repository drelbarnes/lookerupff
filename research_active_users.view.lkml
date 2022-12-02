view: research_active_users {
  derived_table: {
    sql: with

            pe_last as
            (
            select user_id, topic, email, moptin, subscription_status, platform,
            row_number() over (partition by user_id order by timestamp desc) as event_num,
            date(timestamp) as date_stamp, subscription_frequency
            from http_api.purchase_event
            where regexp_contains(user_id, r'^[0-9]*$')
            and user_id <> '0'
            order by user_id, date(timestamp)
            ),

      user_optin as
      (
      select user_id, email, moptin, subscription_status, topic
      from pe_last
      where event_num = 1
      ),

      churn_scores as
      (
      select user_id,
      date(timestamp) as date_stamp,
      churn_prediction_predicted_get_churn_probability_score as churn_score,
      row_number() over (
      partition by user_id
      order by timestamp desc) as row_num
      from looker.get_churn_scores
      ),

      churn_recent as
      (
      select user_id,
      max(row_num) as max_row
      from churn_scores
      group by user_id
      ),

      churn_final as
      (
      select a.*
      from churn_scores as a
      inner join churn_recent as b
      on a.row_num = b.max_row
      and a.user_id = b.user_id
      where churn_score is not null
      ),

      pe_churn as
      (
      select a.*, b.churn_score
      from user_optin as a
      left join churn_final as b
      on a.user_id = b.user_id
      ),

      play_data_global as
      (
      select * from allfirstplay.p0
      where user_id <> '0'
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      ),

      plays_most_granular as
      (
      select user_id,
      row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count,
      timestamp, collection, type, video_id, series,
      title, source, episode, email, winback
      from play_data_global
      order by user_id,
      date(timestamp), video_id, min_count
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
      select a.*, row_number() over (partition by a.user_id order by a.timestamp) as play_number
      from plays_most_granular as a
      inner join plays_max_duration as b
      on a.user_id = b.user_id
      and a.video_id = b.video_id
      and date(a.timestamp) = b.date
      and a.min_count = b.min_count
      ),

      views_in_last_14_days as
      (
      select distinct user_id
      from plays_less_granular
      where date(timestamp) between current_date()-28 and current_date()
      ),

      audience as
      (
      select a.user_id, b.moptin, b.email,
      b.subscription_status, b.topic, b.churn_score
      from views_in_last_14_days as a
      left join pe_churn as b
      on a.user_id = b.user_id
      ),

      final as
      (
      select * from audience where moptin = true and subscription_status = 'enabled'
      )

      select * from final
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: churn_score {
    type: number
    sql: ${TABLE}.churn_score ;;
  }

  set: detail {
    fields: [
      user_id,
      moptin,
      email,
      subscription_status,
      topic,
      churn_score
    ]
  }
}
