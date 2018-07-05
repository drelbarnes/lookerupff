connection: "upff"

# include all the views
include: "*.view"

# include all the dashboards
include: "*.dashboard"

datagroup: upff_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: upff_default_datagroup

explore: ios_users {
  label: "Web to iOS App Users"
  join: javascript_users {
    type:  inner
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
  label: "Web to Android App Users"
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

explore: javascript_users{
  label: "Web Subscribers"
  join: subscribed {
    type:  inner
    sql_on: ${subscribed.user_id} = ${javascript_users.id} ;;
    relationship: one_to_one
  }

  join: android_users {
    type:  inner
    sql_on: ${android_users.id} = ${javascript_users.id} ;;
    relationship: one_to_one
  }

  join: ios_users {
    type:  inner
    sql_on: ${ios_users.id} = ${javascript_users.id} ;;
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

explore: php_subscribers{
  description: "Marketing Opt-In Subs"
  join: analytics {
    type: inner
    sql_on: ${analytics.timestamp_date} = ${php_subscribers.created_date};;
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

explore: purchase_event{label: "Subscribers"}
explore: customers_info_facts{}
