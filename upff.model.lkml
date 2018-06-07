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

explore: javascript_users {
  label: "Web to iOS App Users"
  join: ios_users {
    type:  inner
    sql_on: ${javascript_users.id} = ${ios_users.id} ;;
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

}


explore: analytics{}
explore: customers{}
explore: customers_info_facts{}
