connection: "upff"

# include all the views
include: "*.view"

datagroup: upff_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: upff_default_datagroup

explore: application_installed{
  join: ios_signupstarted {
    type:  left_outer
    sql_on: ${application_installed.anonymous_id} = ${ios_signupstarted.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: analytics{}
explore: analytics_v2 {

  join: customers_v2{
    type:  inner
    sql_on: ${analytics_v2.timestamp_date} = ${customers_v2.creation_timestamp_date};;
    relationship: one_to_many
  }

  join: all_firstplay {
    type:  inner
    sql_on: ${all_firstplay.timestamp_date} = ${analytics_v2.timestamp_date};;
    relationship: one_to_one
  }

}
explore: subscribed {}
explore: pages{}
explore: customers {

  join: customers_analytics {
    type:  inner
    sql_on: ${customers.customer_created_at} = ${customers_analytics.timestamp_date};;
    relationship: many_to_one
  }

  join: android_users {
    type:  inner
    sql_on: ${customers.customer_id} = ${android_users.id};;
    relationship: one_to_one
  }

  join: ios_users {
    type:  inner
    sql_on: ${customers.customer_id} = ${ios_users.id};;
    relationship: one_to_one
  }

}

explore: customers_v2 {
  label: "Subscribers"

  join: analytics_v2 {
    type:  inner
    sql_on: ${customers_v2.event_created_at} = ${analytics_v2.timestamp_date};;
    relationship: many_to_one
  }

  join: all_firstplay {
    type:  inner
    sql_on: ${all_firstplay.user_id} = ${customers_v2.customer_id};;
    relationship: one_to_one
  }

}
explore: churn_reasons_aggregated {}
explore: churn_custom_reasons {}
explore: afinn_lexicon {}
explore: purchase_event {}
explore: http_api_users {}
explore: identifies {}
explore: heartlandia {}
explore: viewership {}
explore: ads_compare {}
explore: lifetime_value {}
explore: churn_texts {}
explore: ltv_cpa {}
explore: customer_churn_percent {}
explore: android_play {}
explore: ios_play {}
explore: javascript_play {}
explore: all_play {

  join: analytics {
    type:  inner
    sql_on: ${analytics.timestamp_date} = ${all_play.timestamp_date} ;;
    relationship: one_to_one
  }

}

explore: all_firstplay {


  join: customers_v2 {
    type:  inner
    sql_on: ${customers_v2.customer_id} = ${all_firstplay.user_id} ;;
    relationship: one_to_one
  }

  join: analytics_v2 {
    type:  inner
    sql_on: ${customers_v2.event_created_at} = ${analytics_v2.timestamp_date};;
    relationship: many_to_one
  }

  join: mailchimp_email_campaigns {
    type:  inner
    sql_on: ${mailchimp_email_campaigns.userid} = ${all_firstplay.user_id};;
    relationship: many_to_one
  }

}

# Web Suscriber Plays
explore: javascript_users {
  label: "Web Subscriber Video ID"

  join: javascript_play {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_play.user_id} ;;
    relationship: one_to_one
  }

  join: all_play {
    type:  inner
    sql_on: ${all_play.user_id} = ${javascript_users.id} ;;
    relationship: one_to_one
  }

}

explore: titles {}
explore: mvpd_subs {}
explore: mtd_revenue {}
explore: upff_linear_ratings {}
explore: uptv_daily_key_demo {}
explore: uptv_daily_day_part {}
