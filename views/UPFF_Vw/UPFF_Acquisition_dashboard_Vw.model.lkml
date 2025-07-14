connection: "upff"

include: "/views/UPFF_Vw/UPFF_analytics_Vw.view.lkml"
include: "/views/UPFF_Vw/rolling.view.lkml"
include: "/views/UPFF_Vw/ios.view.lkml"
explore: UPFF_analytics_Vw {
  label: "UPFF_analytics_Vw"
  join: rolling {
    type: left_outer
    sql_on:  ${rolling.date}=${UPFF_analytics_Vw.date}  ;;
    relationship: many_to_many
  }

  join: ios {
    type: left_outer
    sql_on:  ${ios.report_date_date}=${UPFF_analytics_Vw.date}  ;;
    relationship: many_to_many
  }
}
