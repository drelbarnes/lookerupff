connection: "upff"

include: "/views/customer_info/customer_info.view.lkml"
include: "/views/customer_info/hubspot.view.lkml"
include: "/views/customer_info/opt_in.view.lkml"

explore: customer_info {
  label: "Customer Info"
}

explore: hubspot {
  label: "Customer Info Hubspot"
}

explore: opt_in{
  label: "Subscriber Dashboard : opt in"
}
