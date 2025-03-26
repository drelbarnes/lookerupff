connection: "upff"

include: "/views/Gaither_v2/gaither_analytics_v2.view.lkml"
include: "/views/Gaither_v2/rolling.view.lkml"

explore: gaither_analytics_v2 {
  label: "Gaither Analytics V2"

  join: rolling {
    type: left_outer
    sql_on:  ${rolling.date}=${gaither_analytics_v2.date}  ;;
    relationship: many_to_many
  }
}
