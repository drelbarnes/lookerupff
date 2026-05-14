# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# This mirror uses views/social_performance/social_performance.model.lkml → model: social_performance on all filters and tiles.
# UPTV production: if explores live in upff.model.lkml instead, change every "social_performance" below to upff.

- dashboard: social_performance
  title: "Social Performance Dashboard"
  layout: newspaper

  filters:
    - name: agorapulse_snapshot_date
      title: "Agorapulse snapshot / publish date"
      type: field_filter
      model: social_performance
      explore: social_daily_snapshot
      field: social_daily_snapshot.snapshot_date_date
      default_value: "last 30 days"

    # Use brand_canonical (not raw brand) so UPFF + UP Faith & Family roll up to one filter value.
    - name: brand
      title: "Brand"
      type: field_filter
      model: social_performance
      explore: social_daily_snapshot
      field: social_daily_snapshot.brand_canonical

    - name: platform
      title: "Platform"
      type: field_filter
      model: social_performance
      explore: social_daily_snapshot
      field: social_daily_snapshot.platform

    - name: marketing_attribution_attribution_model
      title: "Marketing Attribution Test Attribution Model"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_test
      field: marketing_attribution_test.attribution_model

    - name: marketing_attribution_attribution_window
      title: "Marketing Attribution Test Attribution Window"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_test
      field: marketing_attribution_test.attribution_window_days

    - name: marketing_attribution_campaign_name
      title: "Marketing Attribution Test Campaign Name"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_test
      field: marketing_attribution_test.campaign_name

  elements:
    - name: total_posts_kpi
      title: "Total posts"
      model: social_performance
      explore: agorapulse_post_performance
      type: single_value
      row: 0
      col: 0
      width: 4
      height: 4
      measures: [agorapulse_post_performance.total_posts]
      note:
        text: "Count of distinct post_id from social_post_snapshot where publish date (publishing_date) falls in the date filter. Audience tiles use snapshot reporting date on social_daily_snapshot."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: total_impressions_kpi
      title: "Total impressions"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 0
      col: 4
      width: 4
      height: 4
      measures: [social_daily_snapshot.total_impressions]
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: total_video_views_kpi
      title: "Total video views"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 0
      col: 8
      width: 4
      height: 4
      measures: [social_daily_snapshot.total_video_views]
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: engagement_rate_kpi
      title: "Engagement rate"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 0
      col: 12
      width: 4
      height: 4
      measures: [social_daily_snapshot.avg_engagement_rate]
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: organic_social_site_visits_kpi
      title: "Organic social site visits"
      model: social_performance
      explore: marketing_attribution_test
      type: single_value
      row: 4
      col: 0
      width: 4
      height: 4
      measures: [marketing_attribution_test.total_visits]
      filters:
        marketing_attribution_test.marketing_platform: "Organic Social"
        marketing_attribution_test.surface: "web"
      note:
        text: "Marketing attribution PDT: page_visit rows, marketing_platform = Organic Social, surface = web. Date filter → report_date (not Agorapulse snapshot)."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_test.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_test.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_test.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_test.campaign_name

    - name: organic_social_free_trials_started_kpi
      title: "Free trials started (organic social, web)"
      model: social_performance
      explore: marketing_attribution_test
      type: single_value
      row: 4
      col: 4
      width: 4
      height: 4
      measures: [marketing_attribution_test.web_trials_started]
      filters:
        marketing_attribution_test.marketing_platform: "Organic Social"
        marketing_attribution_test.surface: "web"
      note:
        text: "Web free trials with primary attribution (default last-touch) within attribution window; filtered to Organic Social in attribution PDT."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_test.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_test.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_test.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_test.campaign_name

    - name: organic_social_trial_to_paid_kpi
      title: "Trial to paid conversion rate (organic social, web)"
      model: social_performance
      explore: marketing_attribution_test
      type: single_value
      row: 4
      col: 8
      width: 4
      height: 4
      measures: [marketing_attribution_test.trial_to_paid_conversion_rate]
      filters:
        marketing_attribution_test.marketing_platform: "Organic Social"
        marketing_attribution_test.surface: "web"
      note:
        text: "free_trials_converted ÷ web_trials_started for filtered rows; Explore default attribution model (parameters) applies."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_test.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_test.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_test.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_test.campaign_name

    - name: organic_social_free_trials_converted_kpi
      title: "Free trials converted (organic social, web)"
      model: social_performance
      explore: marketing_attribution_test
      type: single_value
      row: 4
      col: 12
      width: 4
      height: 4
      measures: [marketing_attribution_test.free_trials_converted]
      filters:
        marketing_attribution_test.marketing_platform: "Organic Social"
        marketing_attribution_test.surface: "web"
      note:
        text: "Distinct users activated from free trial under same attribution filters; Organic Social + web only."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_test.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_test.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_test.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_test.campaign_name

    - name: organic_social_reacquisitions_kpi
      title: "Reacquisitions (organic social, web)"
      model: social_performance
      explore: marketing_attribution_test
      type: single_value
      row: 4
      col: 16
      width: 4
      height: 4
      measures: [marketing_attribution_test.reacquisitions]
      filters:
        marketing_attribution_test.marketing_platform: "Organic Social"
        marketing_attribution_test.surface: "web"
      note:
        text: "Reacquisition conversion rows with primary attribution; Organic Social + web only."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_test.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_test.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_test.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_test.campaign_name

    - name: impressions_over_time
      title: "Impressions over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 8
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: video_views_over_time
      title: "Video views over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 18
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_video_views]
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: brand_performance_summary
      title: "Brand performance summary"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_bar
      row: 28
      col: 0
      width: 12
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
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    # Horizontal bar: distinct posts by brand (publish date + filters). Matches KPI “Total posts” logic.
    - name: posts_by_brand
      title: "Posts by brand"
      model: social_performance
      explore: agorapulse_post_performance
      type: looker_bar
      row: 38
      col: 0
      width: 24
      height: 10
      dimensions: [agorapulse_post_performance.brand_canonical]
      measures: [agorapulse_post_performance.total_posts]
      sorts: [agorapulse_post_performance.total_posts desc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      note:
        text: "Distinct post_id per brand from social_post_snapshot for posts whose publishing_date falls in the date filter (same definition as the Total posts KPI). Horizontal bars compare volume across brands."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: platform_top_channels_impressions
      title: "Top channels by impressions"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_bar
      row: 28
      col: 12
      width: 12
      height: 10
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
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform
