# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# This mirror uses views/social_performance/social_performance.model.lkml → model: social_performance on all filters and tiles.
# UPTV production: if explores live in upff.model.lkml instead, change every "social_performance" below to upff.

- dashboard: social_performance
  title: "Social Performance Dashboard"
  layout: newspaper

  filters:
    - name: agorapulse_snapshot_date
      title: "Social Posts Publish Date Range"
      type: field_filter
      model: social_performance
      explore: social_daily_snapshot
      field: social_daily_snapshot.snapshot_date_date
      default_value: "last 30 days"

    - name: marketing_attribution_attribution_window
      title: "Attribution Window"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.attribution_window_days

    - name: marketing_attribution_attribution_model
      title: "Attribution Model"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.attribution_model



    - name: marketing_attribution_campaign_name
      title: "Campaign Name"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.campaign_name

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

  elements:
    - name: page_summary
      type: text
      title_text: '<div style="text-align: left" align="left">Summary</div>'
      body_text: '<div style="text-align: left" align="left">The Social Performance Dashboard shows how social content performs across Facebook, Instagram, TikTok, and YouTube, and how organic social activity contributes to website visits and subscriptions. The top tiles summarize posts published, followers gained, impressions, video views, engagements, and engagement rate, followed by organic social visits, trials, conversions, and reacquisitions. The charts below highlight top-performing posts, daily performance trends, audience growth, and results by brand and platform. Use the filters to change the date range, brand, platform, campaign, or attribution settings. Read the tile notes before drawing conclusions, especially because YouTube impressions are unavailable and post metrics use publish dates while audience metrics use reporting dates.</div>'
      row: 0
      col: 0
      width: 24
      height: 6

    - name: total_posts_kpi
      title: "Total posts"
      model: social_performance
      explore: agorapulse_post_performance
      type: single_value
      row: 6
      col: 0
      width: 4
      height: 4
      measures: [agorapulse_post_performance.total_posts]
      note:
        text: "How many unique posts were published in the selected dates, using each post's Eastern publish date. Audience tiles use the reporting date, which can fall on a different day than the publish date."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: followers_gained_kpi
      title: "Followers gained"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 6
      col: 4
      width: 4
      height: 4
      measures: [social_daily_snapshot.followers_gained_total]
      note:
        text: "New followers added during the selected dates. On YouTube, this is new subscribers. Unfollows are excluded, and a person who follows more than one account is counted on each account."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: total_impressions_kpi
      title: "Total impressions"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 6
      col: 8
      width: 4
      height: 4
      measures: [social_daily_snapshot.total_impressions_display]
      note:
        text: "Times posts were shown on Facebook, Instagram, and TikTok during the selected dates. YouTube impressions are unavailable, so this shows N/A when only YouTube is selected. YouTube watches are on Total video views."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: total_video_views_kpi
      title: "Total video views"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 6
      col: 12
      width: 4
      height: 4
      measures: [social_daily_snapshot.total_video_views]
      note:
        text: "Times videos were watched during the selected dates on Facebook, Instagram, TikTok, and YouTube. Each day in the range is added together."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: total_engagements_kpi
      title: "Total engagements"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 6
      col: 16
      width: 4
      height: 4
      measures: [social_daily_snapshot.total_engagements]
      note:
        text: "Likes, comments, shares, and other reactions during the selected dates, added together. This is the count of interactions."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: engagement_rate_kpi
      title: "Engagement rate per view"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 6
      col: 20
      width: 4
      height: 4
      measures: [social_daily_snapshot.avg_engagement_rate]
      note:
        text: "The average of each day's engagement rate, so a busy day weighs the same as a quiet day. Facebook, Instagram, and TikTok use Agorapulse's rate. YouTube uses that day's engagements divided by video views."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: organic_social_site_visits_kpi
      title: "Organic social site visits"
      model: social_performance
      explore: marketing_attribution_testv2
      type: single_value
      row: 10
      col: 0
      width: 5
      height: 4
      measures: [marketing_attribution_testv2.total_visits]
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      note:
        text: "Website visits that came from an organic social link. The date is when the visit happened. Results follow the attribution model and window selected above."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: organic_social_free_trials_started_kpi
      title: "Free trials started (organic social, web)"
      model: social_performance
      explore: marketing_attribution_testv2
      type: single_value
      row: 10
      col: 5
      width: 5
      height: 4
      measures: [marketing_attribution_testv2.web_trials_started]
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      note:
        text: "Website free trials credited to organic social. A trial counts when the selected attribution model gives organic social the credit and the trial falls inside the attribution window."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: organic_social_trial_to_paid_kpi
      title: "Trial to paid conversion rate (organic social, web)"
      model: social_performance
      explore: marketing_attribution_testv2
      type: single_value
      row: 10
      col: 10
      width: 5
      height: 4
      measures: [marketing_attribution_testv2.trial_to_paid_conversion_rate]
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      note:
        text: "Share of organic-social website trials that became paying subscribers: trials converted divided by trials started. Follows the attribution model and window selected above."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: organic_social_free_trials_converted_kpi
      title: "Free trials converted (organic social, web)"
      model: social_performance
      explore: marketing_attribution_testv2
      type: single_value
      row: 10
      col: 15
      width: 5
      height: 4
      measures: [marketing_attribution_testv2.free_trials_converted]
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      note:
        text: "People who started a website trial from organic social and then became paying subscribers. Each person is counted once. Follows the attribution model and window selected above."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: organic_social_reacquisitions_kpi
      title: "Reacquisitions (organic social, web)"
      model: social_performance
      explore: marketing_attribution_testv2
      type: single_value
      row: 10
      col: 20
      width: 4
      height: 4
      measures: [marketing_attribution_testv2.reacquisitions]
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      note:
        text: "Former subscribers who resubscribed when organic social gets the credit. Website only. Follows the attribution model and window selected above."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: campaign_subscription_events_organic_social
      title: "Campaign Subscription Events (Organic Social)"
      model: social_performance
      explore: marketing_attribution_testv2
      type: looker_grid
      row: 14
      col: 0
      width: 24
      height: 14
      dimensions:
        - marketing_attribution_testv2.brand_canonical
        - marketing_attribution_testv2.campaign_name
        - marketing_attribution_testv2.campaign_content
        - marketing_attribution_testv2.marketing_platform
        - marketing_attribution_testv2.campaign_medium
      measures:
        - marketing_attribution_testv2.free_trials_started
        - marketing_attribution_testv2.free_trials_converted
        - marketing_attribution_testv2.reacquisitions
      filters:
        marketing_attribution_testv2.marketing_platform: "Organic Social"
        marketing_attribution_testv2.surface: "web"
      sorts:
        - marketing_attribution_testv2.free_trials_started desc
      limit: 200
      note:
        text: "Organic social website results by campaign: trials started, trials that became paid, and subscribers who came back. Uses the conversion date plus the attribution model, window, campaign, and brand filters."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
        marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
        marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
        marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
        brand: marketing_attribution_testv2.brand_canonical

    - name: top_posts_by_impressions
      title: "Top 20 posts by impressions (publish window)"
      model: social_performance
      explore: agorapulse_post_performance
      type: looker_grid
      row: 28
      col: 0
      width: 24
      height: 10
      dimensions:
        - agorapulse_post_performance.post_text
        - agorapulse_post_performance.publishing_date
        - agorapulse_post_performance.brand_canonical
        - agorapulse_post_performance.platform
        - agorapulse_post_performance.post_url
        - agorapulse_post_performance.post_id
      measures:
        - agorapulse_post_performance.post_organic_impressions
        - agorapulse_post_performance.post_paid_impressions
        - agorapulse_post_performance.post_engagements
        - agorapulse_post_performance.post_video_views
      hidden_fields: [agorapulse_post_performance.post_impressions]
      sorts:
        - agorapulse_post_performance.post_impressions desc
        - agorapulse_post_performance.post_video_views desc
      limit: 20
      note:
        text: "The 20 posts with the most impressions among posts published in the selected dates (Eastern time). Organic and paid impressions are each the post's all-time total, so they include performance outside those dates. YouTube shows N/A on both columns. TikTok paid impressions are 0."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: impressions_over_time
      title: "Organic Impressions over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 38
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.organic_impressions_excluding_youtube]
      filters:
        social_daily_snapshot.platform_reports_impressions: "yes"
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      show_null_points: false
      note:
        text: "Organic impressions by day and platform for the selected dates. Facebook and Instagram count organic views only. TikTok is treated as organic. YouTube is hidden because impressions are unavailable. YouTube watches are on the video views chart below."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: video_views_over_time
      title: "Video views over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 48
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_video_views]
      filters:
        social_daily_snapshot.platform_reports_video_views: "yes"
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      note:
        text: "Video watches by day and platform for the selected dates, including YouTube. Instagram is hidden. Each point is that day's watches."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: followers_subscribers_trend
      title: "Followers / subscribers trend by brand and platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_line
      row: 58
      col: 0
      width: 24
      height: 10
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.brand_platform]
      measures: [social_daily_snapshot.followers_on_day]
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      stacking: ""
      show_null_points: false
      note:
        text: "Audience size at the end of each day, by brand and platform. YouTube lines are subscribers. Read each point as that day's size. A gap means no snapshot was captured that day."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: brand_performance_summary
      title: "Brand performance summary"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_bar
      row: 68
      col: 0
      width: 12
      height: 10
      dimensions: [social_daily_snapshot.brand_canonical]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.total_impressions_rank asc]
      note:
        text: "Impressions by brand for the selected dates. YouTube is left out because impressions are unavailable for that platform."
        state: collapsed
        display: hover
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
      row: 78
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
        text: "Unique posts each brand published in the selected dates, using Eastern publish dates. This matches the Total posts tile."
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
      row: 68
      col: 12
      width: 12
      height: 10
      dimensions: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_impressions]
      sorts: [social_daily_snapshot.total_impressions_rank desc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      note:
        text: "Impressions by platform for the selected dates, with the largest platform first. YouTube is left out because impressions are unavailable."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: how_to_read
      type: text
      title_text: '<div style="text-align: left" align="left">How to read this dashboard</div>'
      body_text: |
        <div style="text-align: left" align="left">
        This page has two parts. The top row and the charts below the campaign table come from Agorapulse and describe posts, audience, and engagement. The second row of tiles and the campaign table describe website visits and subscriptions credited to organic social. Those website tiles are website only.
        <br><br>
        <strong>Start with the filters</strong>
        <br><br>
        Set the date range, brand, and platform first, then read the social tiles and charts. Brand applies to every tile. UP Faith & Family and UPFF are one brand. Platform applies to the social tiles and charts only. It leaves the website row and the campaign table unchanged.
        <br><br>
        Attribution model, attribution window, and campaign name apply only to the website row and the campaign table. Changing them leaves posts, impressions, video views, engagements, and followers unchanged.
        <br><br>
        <strong>The date range means three different things</strong>
        <ul>
        <li><strong>Total posts</strong>, <strong>Posts by brand</strong>, and which posts appear in <strong>Top 20 posts by impressions</strong> use each post's Eastern publish date.</li>
        <li><strong>Impressions</strong>, <strong>video views</strong>, <strong>engagements</strong>, <strong>engagement rate</strong>, and <strong>followers gained</strong> use the day the activity happened. A post published earlier can still add to these totals.</li>
        <li>The website tiles and the campaign table use the day of the visit or subscription event, then keep only the events the selected attribution model and window credit to organic social.</li>
        </ul>
        <strong>How to read the numbers</strong>
        <ul>
        <li><strong>Top 20 posts by impressions</strong> ranks posts published in the selected dates. Each impression, engagement, and video-view number is that post's all-time total.</li>
        <li><strong>Engagement rate per view</strong> is the average of each day's rate, so a quiet day counts the same as a busy day. Facebook, Instagram, and TikTok use Agorapulse's rate. YouTube uses that day's engagements divided by video views.</li>
        <li><strong>Followers gained</strong> counts new follows added during the dates. On YouTube that is new subscribers. Unfollows are left out. Someone who follows more than one account is counted on each account.</li>
        <li><strong>Followers / subscribers trend</strong> is the audience size at the end of each day, by brand and platform. A gap means no snapshot was captured that day.</li>
        <li>YouTube impressions are unavailable. <strong>Total impressions</strong> shows N/A when only YouTube is selected, and YouTube is left off the impression charts and the brand and channel bars. Use <strong>Total video views</strong> and the video-views chart for YouTube.</li>
        <li>Twitter/X is not on this dashboard.</li>
        <li>Website visits and trials follow a different count than impressions. Impressions count times a post was shown. The website tiles count visits and subscriptions that arrived from an organic social link and received credit under the attribution settings above the page.</li>
        <li>Numbers refresh once a day. The last few days can still move as Agorapulse revises them.</li>
        </ul>
        Hover the note on any tile when a figure looks off. The note states the rule for that tile.
        </div>
      row: 88
      col: 0
      width: 24
      height: 22
# model: must match the .model.lkml where explore social_daily_snapshot is defined.
# This mirror uses views/social_performance/social_performance.model.lkml → model: social_performance on all filters and tiles.
# UPTV production: if explores live in upff.model.lkml instead, change every "social_performance" below to upff.

- dashboard: social_performance_v2
  title: "Social Performance Dashboard v2"
  layout: newspaper

  filters:
    - name: agorapulse_snapshot_date
      title: "Social Posts Publish Date Range"
      type: field_filter
      model: social_performance
      explore: social_daily_snapshot
      field: social_daily_snapshot.snapshot_date_date
      default_value: "last 30 days"

    - name: marketing_attribution_attribution_window
      title: "Attribution Window"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.attribution_window_days

    - name: marketing_attribution_attribution_model
      title: "Attribution Model"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.attribution_model



    - name: marketing_attribution_campaign_name
      title: "Campaign Name"
      type: field_filter
      model: social_performance
      explore: marketing_attribution_testv2
      field: marketing_attribution_testv2.campaign_name

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

  elements:
    - name: page_summary
      type: text
      title_text: '<div style="text-align: left; margin: 0; padding: 0; line-height: 1.2;" align="left">Summary</div>'
      body_text: '<div style="text-align: left; margin: 0; padding: 0; line-height: 1.35;" align="left">The Social Performance Dashboard shows how social content performs across Facebook, Instagram, TikTok, and YouTube, and how organic social activity contributes to website visits and subscriptions. The top tiles summarize posts published, followers gained, impressions, video views, engagements, and engagement rate, followed by organic social visits, trials, conversions, and reacquisitions. The charts below highlight top-performing posts, daily performance trends, audience growth, and results by brand and platform. Use the filters to change the date range, brand, platform, campaign, or attribution settings. Read the tile notes before drawing conclusions, especially because YouTube impressions are unavailable and post metrics use publish dates while audience metrics use reporting dates.</div>'
      row: 0
      col: 0
      width: 24
      height: 4

    - name: total_posts_kpi
      title: "Total posts"
      model: social_performance
      explore: agorapulse_post_performance
      type: single_value
      row: 4
      col: 0
      width: 6
      height: 4
      measures: [agorapulse_post_performance.total_posts]
      note:
        text: "How many unique posts were published in the selected dates, using each post's Eastern publish date. Audience tiles use the reporting date, which can fall on a different day than the publish date."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: followers_gained_kpi
      title: "Followers gained"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 4
      col: 6
      width: 6
      height: 4
      measures: [social_daily_snapshot.followers_gained_total]
      note:
        text: "New followers added during the selected dates. On YouTube, this is new subscribers. Unfollows are excluded, and a person who follows more than one account is counted on each account."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    # - name: total_impressions_kpi
    #   title: "Total impressions"
    #   model: social_performance
    #   explore: social_daily_snapshot
    #   type: single_value
    #   row: 4
    #   col: 8
    #   width: 4
    #   height: 4
    #   measures: [social_daily_snapshot.total_impressions_display]
    #   note:
    #     text: "Times posts were shown on Facebook, Instagram, and TikTok during the selected dates. YouTube impressions are unavailable, so this shows N/A when only YouTube is selected. YouTube watches are on Total video views."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
    #     brand: social_daily_snapshot.brand_canonical
    #     platform: social_daily_snapshot.platform

    # - name: total_video_views_kpi
    #   title: "Total video views"
    #   model: social_performance
    #   explore: social_daily_snapshot
    #   type: single_value
    #   row: 4
    #   col: 12
    #   width: 4
    #   height: 4
    #   measures: [social_daily_snapshot.total_video_views]
    #   note:
    #     text: "Times videos were watched during the selected dates on Facebook, Instagram, TikTok, and YouTube. Each day in the range is added together."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
    #     brand: social_daily_snapshot.brand_canonical
    #     platform: social_daily_snapshot.platform

    - name: total_engagements_kpi
      title: "Total engagements"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 4
      col: 12
      width: 6
      height: 4
      measures: [social_daily_snapshot.total_engagements]
      note:
        text: "Likes, comments, shares, and other reactions during the selected dates, added together. This is the count of interactions."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: engagement_rate_kpi
      title: "Engagement rate per view"
      model: social_performance
      explore: social_daily_snapshot
      type: single_value
      row: 4
      col: 18
      width: 6
      height: 4
      measures: [social_daily_snapshot.avg_engagement_rate]
      note:
        text: "The average of each day's engagement rate, so a busy day weighs the same as a quiet day. Facebook, Instagram, and TikTok use Agorapulse's rate. YouTube uses that day's engagements divided by video views."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    # - name: organic_social_site_visits_kpi
    #   title: "Organic social site visits"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: single_value
    #   row: 8
    #   col: 0
    #   width: 5
    #   height: 4
    #   measures: [marketing_attribution_testv2.total_visits]
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   note:
    #     text: "Website visits that came from an organic social link. The date is when the visit happened. Results follow the attribution model and window selected above."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    # - name: organic_social_paid_trials_kpi
    #   title: "Paid trials (organic social, web)"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: single_value
    #   row: 8
    #   col: 5
    #   width: 5
    #   height: 4
    #   measures: [marketing_attribution_testv2.paid_trials]
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   note:
    #     text: "Website trials credited to organic social. This is the free-trial start count. A trial counts when organic social gets the credit and it falls inside the attribution window."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    # - name: organic_social_trial_to_paid_kpi
    #   title: "Trial to paid conversion rate (organic social, web)"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: single_value
    #   row: 8
    #   col: 10
    #   width: 5
    #   height: 4
    #   measures: [marketing_attribution_testv2.trial_to_paid_conversion_rate]
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   note:
    #     text: "Share of organic-social website trials that became paying subscribers: trials converted divided by trials started. Follows the attribution model and window selected above."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    # - name: organic_social_free_trials_converted_kpi
    #   title: "Free trials converted (organic social, web)"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: single_value
    #   row: 8
    #   col: 15
    #   width: 5
    #   height: 4
    #   measures: [marketing_attribution_testv2.free_trials_converted]
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   note:
    #     text: "People who started a website trial from organic social and then became paying subscribers. Each person is counted once. Follows the attribution model and window selected above."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    # - name: organic_social_reacquisitions_kpi
    #   title: "Reacquisitions (organic social, web)"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: single_value
    #   row: 8
    #   col: 20
    #   width: 4
    #   height: 4
    #   measures: [marketing_attribution_testv2.reacquisitions]
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   note:
    #     text: "Former subscribers who resubscribed when organic social gets the credit. Website only. Follows the attribution model and window selected above."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    # - name: campaign_subscription_events_organic_social
    #   title: "Campaign Subscription Events (Organic Social)"
    #   model: social_performance
    #   explore: marketing_attribution_testv2
    #   type: looker_grid
    #   row: 12
    #   col: 0
    #   width: 24
    #   height: 14
    #   dimensions:
    #     - marketing_attribution_testv2.brand_canonical
    #     - marketing_attribution_testv2.campaign_name
    #     - marketing_attribution_testv2.campaign_content
    #     - marketing_attribution_testv2.marketing_platform
    #     - marketing_attribution_testv2.campaign_medium
    #   measures:
    #     - marketing_attribution_testv2.paid_trials
    #     - marketing_attribution_testv2.free_trials_converted
    #     - marketing_attribution_testv2.reacquisitions
    #   filters:
    #     marketing_attribution_testv2.marketing_platform: "Organic Social"
    #     marketing_attribution_testv2.surface: "web"
    #   sorts:
    #     - marketing_attribution_testv2.paid_trials desc
    #   limit: 200
    #   note:
    #     text: "Organic social website results by campaign: trials started, trials that became paid, and subscribers who came back. Uses the conversion date plus the attribution model, window, campaign, and brand filters."
    #     state: collapsed
    #     display: hover
    #   listen:
    #     agorapulse_snapshot_date: marketing_attribution_testv2.report_date_date
    #     marketing_attribution_attribution_model: marketing_attribution_testv2.attribution_model
    #     marketing_attribution_attribution_window: marketing_attribution_testv2.attribution_window_days
    #     marketing_attribution_campaign_name: marketing_attribution_testv2.campaign_name
    #     brand: marketing_attribution_testv2.brand_canonical

    - name: top_posts_by_impressions
      title: "Top 20 posts by impressions (publish window)"
      model: social_performance
      explore: agorapulse_post_performance
      type: looker_grid
      row: 8
      col: 0
      width: 24
      height: 10
      dimensions:
        - agorapulse_post_performance.post_text
        - agorapulse_post_performance.publishing_date
        - agorapulse_post_performance.brand_canonical
        - agorapulse_post_performance.platform
        - agorapulse_post_performance.post_url
        - agorapulse_post_performance.post_id
      measures:
        - agorapulse_post_performance.post_organic_impressions
        - agorapulse_post_performance.post_paid_impressions
        - agorapulse_post_performance.post_engagements
        - agorapulse_post_performance.post_video_views
      hidden_fields: [agorapulse_post_performance.post_impressions]
      sorts:
        - agorapulse_post_performance.post_impressions desc
        - agorapulse_post_performance.post_video_views desc
      limit: 20
      note:
        text: "The 20 posts with the most impressions among posts published in the selected dates (Eastern time). Organic and paid impressions are each the post's all-time total, so they include performance outside those dates. YouTube shows N/A on both columns. TikTok paid impressions are 0."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: impressions_over_time
      title: "Organic Impressions over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 18
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.organic_impressions_excluding_youtube]
      filters:
        social_daily_snapshot.platform_reports_impressions: "yes"
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      show_null_points: false
      note:
        text: "Organic impressions by day and platform for the selected dates. Facebook and Instagram count organic views only. TikTok is treated as organic. YouTube is hidden because impressions are unavailable. YouTube watches are on the video views chart below."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: video_views_over_time
      title: "Video views over time by platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_area
      row: 28
      col: 0
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.total_video_views]
      filters:
        social_daily_snapshot.platform_reports_video_views: "yes"
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      width: 24
      height: 10
      stacking: ""
      note:
        text: "Video watches by day and platform for the selected dates, including YouTube. Instagram is hidden. Each point is that day's watches."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: followers_subscribers_trend
      title: "Followers / subscribers trend by brand and platform"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_line
      row: 38
      col: 0
      width: 24
      height: 10
      dimensions: [social_daily_snapshot.snapshot_date_date]
      pivots: [social_daily_snapshot.brand_platform]
      measures: [social_daily_snapshot.followers_on_day]
      sorts: [social_daily_snapshot.snapshot_date_date asc]
      x_axis_scale: auto
      stacking: ""
      show_null_points: false
      note:
        text: "Audience size at the end of each day, by brand and platform. YouTube lines are subscribers. Read each point as that day's size. A gap means no snapshot was captured that day."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: brand_performance_summary
      title: "Brand performance summary for organic"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_bar
      row: 48
      col: 0
      width: 12
      height: 10
      dimensions: [social_daily_snapshot.brand_canonical]
      measures: [social_daily_snapshot.organic_impressions_excluding_youtube]
      sorts: [social_daily_snapshot.organic_impressions_excluding_youtube desc]
      note:
        text: "Organic impressions by brand for the selected dates. Facebook and Instagram count organic views only. TikTok is treated as organic. YouTube is left out because impressions are unavailable."
        state: collapsed
        display: hover
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
      row: 58
      col: 0
      width: 24
      height: 5
      dimensions: [agorapulse_post_performance.brand_canonical]
      measures: [agorapulse_post_performance.total_posts]
      sorts: [agorapulse_post_performance.total_posts desc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      note:
        text: "Unique posts each brand published in the selected dates, using Eastern publish dates. This matches the Total posts tile."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: agorapulse_post_performance.publishing_date
        brand: agorapulse_post_performance.brand_canonical
        platform: agorapulse_post_performance.platform

    - name: platform_top_channels_impressions
      title: "Top channels by impressions for organic"
      model: social_performance
      explore: social_daily_snapshot
      type: looker_bar
      row: 48
      col: 12
      width: 12
      height: 10
      dimensions: [social_daily_snapshot.platform]
      measures: [social_daily_snapshot.organic_impressions_excluding_youtube]
      filters:
        social_daily_snapshot.platform_reports_impressions: "yes"
      sorts: [social_daily_snapshot.organic_impressions_excluding_youtube desc]
      stacking: ""
      hide_legend: true
      show_value_labels: true
      x_axis_gridlines: false
      y_axis_gridlines: false
      note:
        text: "Organic impressions by platform for the selected brand and dates, with the largest platform first. Facebook and Instagram count organic views only. TikTok is treated as organic. YouTube is left out because impressions are unavailable."
        state: collapsed
        display: hover
      listen:
        agorapulse_snapshot_date: social_daily_snapshot.snapshot_date_date
        brand: social_daily_snapshot.brand_canonical
        platform: social_daily_snapshot.platform

    - name: how_to_read
      type: text
      title_text: '<div style="text-align: left" align="left">How to read this dashboard</div>'
      body_text: |
        <div style="text-align: left" align="left">
        This page has two parts. The top row and the charts below the campaign table come from Agorapulse and describe posts, audience, and engagement. The second row of tiles and the campaign table describe website visits and subscriptions credited to organic social. Those website tiles are website only.
        <br><br>
        <strong>Start with the filters</strong>
        <br><br>
        Set the date range, brand, and platform first, then read the social tiles and charts. Brand applies to every tile. UP Faith & Family and UPFF are one brand. Platform applies to the social tiles and charts only. It leaves the website row and the campaign table unchanged.
        <br><br>
        Attribution model, attribution window, and campaign name apply only to the website row and the campaign table. Changing them leaves posts, impressions, video views, engagements, and followers unchanged.
        <br><br>
        <strong>The date range means three different things</strong>
        <ul>
        <li><strong>Total posts</strong>, <strong>Posts by brand</strong>, and which posts appear in <strong>Top 20 posts by impressions</strong> use each post's Eastern publish date.</li>
        <li><strong>Impressions</strong>, <strong>video views</strong>, <strong>engagements</strong>, <strong>engagement rate</strong>, and <strong>followers gained</strong> use the day the activity happened. A post published earlier can still add to these totals.</li>
        <li>The website tiles and the campaign table use the day of the visit or subscription event, then keep only the events the selected attribution model and window credit to organic social.</li>
        </ul>
        <strong>How to read the numbers</strong>
        <ul>
        <li><strong>Top 20 posts by impressions</strong> ranks posts published in the selected dates. Each impression, engagement, and video-view number is that post's all-time total.</li>
        <li><strong>Engagement rate per view</strong> is the average of each day's rate, so a quiet day counts the same as a busy day. Facebook, Instagram, and TikTok use Agorapulse's rate. YouTube uses that day's engagements divided by video views.</li>
        <li><strong>Followers gained</strong> counts new follows added during the dates. On YouTube that is new subscribers. Unfollows are left out. Someone who follows more than one account is counted on each account.</li>
        <li><strong>Followers / subscribers trend</strong> is the audience size at the end of each day, by brand and platform. A gap means no snapshot was captured that day.</li>
        <li>YouTube impressions are unavailable. <strong>Total impressions</strong> shows N/A when only YouTube is selected, and YouTube is left off the impression charts and the brand and channel bars. Use <strong>Total video views</strong> and the video-views chart for YouTube.</li>
        <li>Twitter/X is not on this dashboard.</li>
        <li>Website visits and trials follow a different count than impressions. Impressions count times a post was shown. The website tiles count visits and subscriptions that arrived from an organic social link and received credit under the attribution settings above the page.</li>
        <li>Numbers refresh once a day. The last few days can still move as Agorapulse revises them.</li>
        </ul>
        Hover the note on any tile when a figure looks off. The note states the rule for that tile.
        </div>
      row: 63
      col: 0
      width: 24
      height: 22
