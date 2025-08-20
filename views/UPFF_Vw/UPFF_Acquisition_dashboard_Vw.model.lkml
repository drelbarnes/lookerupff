connection: "upff"

include: "/views/UPFF_Vw/UPFF_analytics_Vw.view.lkml"
include: "/views/UPFF_Vw/rolling.view.lkml"
include: "/views/UPFF_Vw/ios.view.lkml"
include: "/views/UPFF_Vw/running.view.lkml"
include: "/views/UPFF_Vw/trials_by_platform.view.lkml"
include: "/views/UPFF_Vw/subscriber_data.view.lkml"
include: "/views/UPFF_Vw/vimeo.view.lkml"
include: "/views/UPFF_Vw/free_trials.view.lkml"
include: "/views/UPFF_Vw/free_trials_historical.view.lkml"
include: "/views/UPFF_Vw/converted.view.lkml"
include: "/views/UPFF_Vw/sub_count.view.lkml"
explore: UPFF_analytics_Vw {
  label: "UPFF_analytics_Vw"
  join: rolling {
    type: left_outer
    sql_on:  ${rolling.date}=${UPFF_analytics_Vw.date}  ;;
    relationship: many_to_many
  }

  join: ios {
    type: left_outer
    sql_on:  ${ios.report_date_date}=${UPFF_analytics_Vw.date} and ${ios.billing_period}=${UPFF_analytics_Vw.billing_period} ;;
    relationship: many_to_many
  }

  join: running {
    type: left_outer
    sql_on:  ${running.report_date_date}=${UPFF_analytics_Vw.date}  ;;
    relationship: many_to_many
  }

  join: free_trials {
    type: left_outer
    sql_on: ${free_trials.date}=${UPFF_analytics_Vw.date}  ;;
    relationship: many_to_many
  }

}

explore: converted {
  label: "UPFF Conversion V2"

  join: free_trials_historical {
    type: left_outer
    sql_on: ${free_trials_historical.date}=${converted.date}  ;;
    relationship: many_to_many
  }
}

explore: trials_by_platform {
  label: "UPFF Free Trials VW"
}

explore: subscriber_data {
  label: "UPFF Subscriber Data"
}

explore: vimeo {
  label: "UPFF Resubscribers"
}

explore: sub_count {
  label: "UPFF V2 sub count"
}
