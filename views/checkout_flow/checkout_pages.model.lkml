connection: "upff"

include: "/views/checkout_flow/checkout_pages.view.lkml"
include: "/views/checkout_flow/checkout_pages.view.lkml"
include: "/views/checkout_flow/checkout_pages2.view.lkml"
include: "/views/checkout_flow/checkout_gaitherTV.view.lkml"

explore: checkout_pages{
  label: "Checkout Flow"
}

explore: checkout_gaitherTV{
  label: "Gaither Checkout Flow "
}

explore: checkout_pages2{
  label: "Checkout Flow2"
}
