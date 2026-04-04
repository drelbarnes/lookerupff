# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# UPTV production: explore lives in upff.model.lkml → use model: upff.
# If you use only lookml/models/social_snapshot.model.lkml, change every "upff" below to social_snapshot.

- dashboard: social_performance
  title: "Social Performance Dashboard"
  layout: newspaper

  filters:
    - name: snapshot_date
      title: "Date range"
      type: field_filter
      model: upff
      explore: social_daily_snapshot
      field: social_daily_snapshot.snapshot_date_date
      default_value: "last 30 days"

    - name: brand
      title: "Brand"
      type: field_filter
      model: upff
      explore: social_daily_snapshot
      field: social_daily_snapshot.brand_canonical

    - name: platform
      title: "Platform"
      type: field_filter
      model: upff
      explore: social_daily_snapshot
      field: social_daily_snapshot.platform

  elements:
    - name: total_impressions_kpi
      title: "Total impressions"
      model: upff
      explore: social_daily_snapshot
      type: single_value
      measures: [social_daily_snapshot.total_impressions]
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: total_video_views_kpi
      title: "Total video views"
      model: upff
      explore: social_daily_snapshot
      type: single_value
      measures: [social_daily_snapshot.total_video_views]
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: engagement_rate_kpi
      title: "Engagement rate"
      model: upff
      explore: social_daily_snapshot
      type: single_value
      measures: [social_daily_snapshot.avg_engagement_rate]
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: impressions_over_time
      title: "Impressions over time by platform"
      model: upff
      explore: social_daily_snapshot
      type: looker_area
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 16
      height: 10
      stacking: ""
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: brand_performance_summary
      title: "Brand performance summary"
      model: upff
      explore: social_daily_snapshot
      type: looker_bar
      width: 8
      height: 10
      dimensions: [social_daily_snapshot.brand_canonical]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.total_impressions asc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform
