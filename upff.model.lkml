connection: "upff"

# include all the views
include: "*.view"

datagroup: upff_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: upff_default_datagroup

explore: ios_users {
  label: "Web and iOS App Users"
  join: javascript_users {
    type:  left_outer
    sql_on: ${javascript_users.id} = ${ios_users.id} ;;
    relationship: one_to_one
  }

  join: javascript_identifies {
    type:  inner
    sql_on: ${ios_users.id} = ${javascript_identifies.user_id} ;;
    relationship: one_to_one
  }

}

explore: android_users {
  label: "Web and Android App Users"
  join: javascript_users {
    type:  inner
    sql_on: ${javascript_users.id} = ${android_users.id} ;;
    relationship: one_to_one
  }

  join: javascript_identifies {
    type:  inner
    sql_on: ${android_users.id} = ${javascript_identifies.user_id} ;;
    relationship: one_to_one
  }

}

explore: web_to_ios{
  label: "Web to iOS Subscribers"
  from: subscribed

  join: javascript_users {
    sql_on: ${javascript_users.id} = ${web_to_ios.user_id};;
    relationship: one_to_one
  }


  join: ios_users {
    type: inner
    sql_on: ${javascript_users.id} = ${ios_users.id} ;;
    required_joins: [javascript_users]
    relationship: one_to_one
  }
}

explore: web_to_android{
  label: "Web to Android Subscribers"
  from: subscribed

  join: javascript_users {
    sql_on: ${javascript_users.id} = ${web_to_android.user_id};;
    relationship: one_to_one
  }

  join: android_users {
    type: inner
    sql_on: ${javascript_users.id} = ${android_users.id} ;;
    relationship: one_to_one
  }

}

# Web Suscribers
explore: javascript_subscribed {

  label: "Web Subscribers"
  from: subscribed

  join: javascript_users {
    type:  inner
    sql_on: ${javascript_subscribed.user_id} = ${javascript_users.id} ;;
    relationship: one_to_one
  }

}

# Web Suscriber Plays
explore: javascript_users {

  label: "Web Subscriber Plays"

  join: javascript_play {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_play.user_id} ;;
    relationship: one_to_one
  }

  join: javascript_firstplay {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_firstplay.user_id} ;;
    relationship: one_to_one
  }

}

explore: javascript_uptv_pages {
  label: "Cross-Domain Subs"
  join: subscribed {
    type:  inner
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${subscribed.context_traits_cross_domain_id} ;;
    relationship: one_to_one
  }

  join: javascript_users {
    type:  inner
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${javascript_users.context_traits_cross_domain_id} ;;
    relationship: one_to_one
  }

  join: javascript_play {
    type: inner
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${javascript_play.context_traits_cross_domain_id};;
    relationship: one_to_one
  }
}


explore: analytics{}

explore: php_get_customers{
  label: "Mktg Opt-In Subscribers"
  description: "Marketing Opt-In Subs"
  join: analytics {
    type: inner
    sql_on: ${analytics.timestamp_date} = ${php_get_customers.created_date};;
    relationship: one_to_one
  }
}

explore: customers{


  join: android_users {
    type:  left_outer
    sql_on: ${customers.customer_id} = ${android_users.id};;
    relationship: one_to_one
  }

  join: ios_users {
    type:  left_outer
    sql_on: ${customers.customer_id} = ${ios_users.id};;
    relationship: one_to_one
  }

}

explore: subscribed {}
explore: purchase_event{label: "Subscribers"}
explore: customers_info_facts{}


#Delighted.com // Feedback Survey Responses
explore: delighted_survey_question_answered {
  label: "Delighted Feedback"

  join: customers_v2 {
    type: inner
    sql_on: ${delighted_survey_question_answered.user_id} = ${customers_v2.customer_id};;
    relationship: one_to_one
  }

  join: analytics_v2 {
    type:  inner
    sql_on: ${customers_v2.event_created_at} = ${analytics_v2.timestamp_date};;
    relationship: many_to_one
  }

  join: all_firstplay {
    type:  inner
    sql_on: ${all_firstplay.timestamp_date} = ${analytics_v2.timestamp_date};;
    relationship: one_to_one
  }

  join: mailchimp_email_campaigns {
    type:  inner
    sql_on: ${mailchimp_email_campaigns.campaign_date} = ${delighted_survey_question_answered.timestamp_date};;
    relationship: one_to_one
  }


}

#iOS // get user plays
explore: ios_users_firstplay {
  label: "iOS Subscribers Play"
  from:  ios_users

  join: ios_firstplay {
    type: inner
    sql_on: ${ios_users_firstplay.id} = ${ios_firstplay.user_id};;
    relationship: one_to_one
  }
}

#Android // get user plays
explore: android_users_play {
  label: "Android Subscribers Play"
  from:  android_users

  join: android_play {
    type: inner
    sql_on: ${android_users_play.id} = ${android_play.user_id};;
    relationship: one_to_one
  }
}

explore: javascript_pages {label: "Web Pages Views"}
explore: ios_view {label: "iOS Views"}
explore: android_signin {label: "Android Sign-in"}
explore: ios_signin { label: "iOS Sign-in"}
explore: android_signupstarted {label: "Android Signupstarted"}
explore: ios_signupstarted { label: "iOS Signupstarted"}
explore: javascript_timeupdate {label: "Web Timeupdate"}
explore: ios_timeupdate {}
explore: android_timeupdate {}
explore: javascript_authentication {label: "Web Authentication"}
explore: javascript_derived_timeupdate {}
explore: derived_marketing_attribution {label: "Attribution: Cross Platform"}
