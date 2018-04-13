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

explore: application_installed{
  join: signupstarted {
    type:  left_outer
    sql_on: ${application_installed.anonymous_id} = ${signupstarted.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: analytics{

}

explore: customers{
  label: "Subscribers"
  join: play {
    type:  left_outer
    sql_on: ${customers.customer_id} = ${play.user_id} ;;
    relationship: one_to_many
  }

}
