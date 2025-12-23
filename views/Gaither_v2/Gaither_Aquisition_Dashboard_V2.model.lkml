connection: "upff"

include: "/views/Gaither_v2/gaither_analytics_v2.view.lkml"
include: "/views/Gaither_v2/rolling.view.lkml"
include: "/views/Gaither_v2/cpft.view.lkml"
include: "/views/Gaither_v2/chargebee_webhook.view.lkml"
include: "/views/Gaither_v2/vimeo_webhook.view.lkml"
include: "/views/Gaither_v2/subscriber_data.view.lkml"


explore: gaither_analytics_v2 {
  label: "Gaither Analytics V2"

  join: rolling {
    type: left_outer
    sql_on:  ${rolling.date}=${gaither_analytics_v2.date}  ;;
    relationship: many_to_many
  }
}

explore: cpft {
  label: "Gaither CPFT"}


explore: subscriber_data {
  label: "Gaither Subscriber Data"
}
