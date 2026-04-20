# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# UPTV production: explore lives in upff.model.lkml → use model: upff.
# If you use only lookml/models/social_snapshot.model.lkml, change every "upff" below to social_snapshot.

- dashboard: social_performance
  title: "Social Performance Dashboard"
  layout: newspaper

  filters:
    - name: snapshot_date
      title: "Date range (snapshot / publish)"
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
    - name: total_posts_kpi
      title: "Total posts"
      model: upff
      explore: agorapulse_post_performance
      type: single_value
      measures: [agorapulse_post_performance.total_posts]
      note:
        text: "Count of distinct post_id where publish date (publishingDate) falls in the date filter. Audience tiles use snapshot reporting date on social_daily_snapshot."
        state: collapsed
        display: hover
      listen:
        snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

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

    - name: video_views_over_time
      title: "Video views over time by platform"
      model: upff
      explore: social_daily_snapshot
      type: looker_area
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_video_views]
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

    - name: platform_top_channels_impressions
      title: "Top channels by impressions"
      model: upff
      explore: social_daily_snapshot
      type: looker_bar
      dimensions: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.total_impressions desc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      note:
        text: "Horizontal bar chart (Looker Bar). One bar per platform, sorted by total impressions for the selected date range and brand/platform filters—ranking headline volume (doc 07 §8)."
        state: collapsed
        display: hover
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: platform_impressions_vs_weighted_engagement
      title: "Platform reach vs engagement (weighted)"
      model: upff
      explore: social_daily_snapshot
      type: looker_scatter
      dimensions: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_impressions, social_daily_snapshot.weighted_engagement_rate]
      hidden_fields: [social_daily_snapshot.platform]
      sorts: [social_daily_snapshot.platform asc]
      x_axis_scale: linear
      point_style: circle
      show_value_labels: true
      note:
        text: "Y = weighted engagement rate (sum(engagements) ÷ sum(impressions)); X = total impressions—per platform for the same filters (doc 07 §6 Option B × §8). Surfaces high-reach vs high-engagement tradeoffs; differs from the Engagement rate KPI (avg_engagement_rate, Option A)."
        state: collapsed
        display: hover
      listen:
        snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform
