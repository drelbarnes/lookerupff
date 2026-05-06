connection: "upff"

include: "/views/Marketing_attribution/marketing_attribution.view.lkml"
include: "/views/Marketing_attribution/visits.view.lkml"
include: "/views/Marketing_attribution/blog_attribution.view.lkml"
include: "/views/Marketing_attribution/post_blog_visits.view.lkml"


explore: marketing_attribution {
  label: "Marketing Attribution V2"
}

explore: visits {
  label: "Marketing Attribution V2 visits"
}

explore: blog_attribution {
  label: "Blog Attribution"
}

explore: post_blog_visits {

}
