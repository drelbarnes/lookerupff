project_name: "upff"

# # Use local_dependency: To enable referencing of another project
# # on this instance with include: statements
#
# local_dependency: {
#   project: "name_of_other_project"
# }

remote_dependency: demo_segment {
  url: "https://github.com/llooker/demo_segment.git"
  ref: "master"
}
