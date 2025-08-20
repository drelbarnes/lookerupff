connection: "upff"

include: "/views/v1_debug/v1_debug.view.lkml"                # include all views in the views/
include: "/views/v1_debug/chargebee_webhook_events.view.lkml"
include: "/views/v1_debug/chargebee_vimeo_ott_id_mapping.view.lkml"

include: "/views/customer_file_subscriber_counts.view.lkml"
include: "/views/appstoreconnect_sub_counts.view.lkml"


explore: v1_debug {}
