connection: "upff"

include: "/views/checkout_flow/checkout_pages.view.lkml"
include: "/views/checkout_flow/checkout_pages.view.lkml"
include: "/views/checkout_flow/checkout_pages2.view.lkml"
include: "/views/checkout_flow/checkout_gaitherTV.view.lkml"
include: "/views/checkout_flow/marketing_page_source.view.lkml"

explore: checkout_pages{
  label: "Checkout Flow"
}

explore: checkout_gaitherTV{
  label: "Gaither Checkout Flow "
}

explore: checkout_pages2{
  label: "Checkout Flow2"
}

explore: marketing_page_source {
  label: "Markeing Page by Referrer"
}
