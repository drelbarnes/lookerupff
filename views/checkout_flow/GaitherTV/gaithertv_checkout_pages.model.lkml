connection: "upff"

include: "/views/checkout_flow/GaitherTV/chekout_pages.view.lkml"
include: "/views/checkout_flow/GaitherTV/checkout_pages2.view.lkml"
#include: "/views/checkout_flow/checkout_gaitherTV.view.lkml"
include: "/views/checkout_flow/GaitherTV/marketing_page_source.view.lkml"

explore: checkout_pages{
  label: "GaitherTV+ Checkout Flow"
}

explore: checkout_pages2{
  label: "GaitherTV+ Checkout Flow2"
}

explore: marketing_page_source {
  label: "GaitherTV+ Markeing Page by Referrer"
}
