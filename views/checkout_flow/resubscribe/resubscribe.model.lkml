connection: "upff"

include: "/views/checkout_flow/resubscribe/resubscribe_pages.view.lkml"
include: "/views/checkout_flow/resubscribe/resubscribe_pages2.view.lkml"

include:"/views/checkout_flow/resubscribe/resubscribe_error.view.lkml"

explore: resubscribe_pages{
  label: "Resubscribe Flow"
}

explore: resubscribe_pages2{
  label: "Resubscribe Flow2"
}

explore: resubscribe_error{
  label: "Resubscribe Error"
}
