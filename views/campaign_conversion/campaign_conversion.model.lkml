connection: "upff"

include: "/views/campaign_conversion/campaign_conversion.view.lkml"                #
include: "/views/campaign_conversion/coupon_conversion.view.lkml"                #

explore: campaign_conversion{
  label: "UPFF conversion"

}

explore: coupon_conversion {
  label: "UPFF Coupon Conversion Tracking"
}
