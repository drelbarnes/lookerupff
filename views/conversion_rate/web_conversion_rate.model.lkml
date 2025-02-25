connection: "upff"

include: "/views/conversion_rate/page_visits.view.lkml"
include: "/views/conversion_rate/trials_created.view.lkml"


explore: trials_created {
  label: "Web Conversion Rate"

  join: page_visits {
    type: left_outer
    sql_on: ${trials_created.day} = ${page_visits.time_period} ;;
    relationship: one_to_one
  }
}
