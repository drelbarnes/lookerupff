# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# UPTV production: explore lives in upff.model.lkml → use model: upff.
# If you use only lookml/models/social_snapshot.model.lkml, change every "upff" below to social_snapshot.

- dashboard: social_performance
  title: "Social — Total impressions (MVP)"
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
      field: social_daily_snapshot.brand

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
        brand: social_daily_snapshot.brand
        platform: social_daily_snapshot.platform
