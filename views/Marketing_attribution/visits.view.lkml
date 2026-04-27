view: visits {
  derived_table: {
    sql:
      with result as (SELECT
        date(received_at) as report_date
        ,context_ip
        ,anonymous_id
        ,context_campaign_source as campaign_source
        ,context_campaign_name as campaign_name
        ,context_campaign_medium as campaign_medium
        ,context_campaign_content as campaign_content
        ,context_campaign_term as campaign_term
        ,referrer
        ,CASE
          when context_campaign_source is null and (referrer is null or referrer in ('upfaithandfamily.com/', 'upfaithandfamily.com', 'vhx.tv'))
      then 'unknown'
      when context_campaign_source is null and (referrer is not null and referrer not in ('upfaithandfamily.com/', 'upfaithandfamily.com', 'vhx.tv'))
      then referrer
      else campaign_source
      END AS source

       FROM javascript_upff_home.pages),

      result2 as (
      select *
      , CASE
    WHEN LOWER(source) LIKE '%hs_email%'
      OR LOWER(source) LIKE '%hs_automation%'
      OR LOWER(source) LIKE '%hubspot_upff%'
      OR LOWER(source) LIKE '%hubspot_uptv%'
      OR LOWER(source) LIKE '%hubspot_gtv%'
    THEN 'HubSpot'

    WHEN LOWER(source) LIKE '%fb%'
      OR LOWER(source) LIKE '%facebook%'
      OR LOWER(source) LIKE '%ig%'
      OR LOWER(source) LIKE '%an%'
      OR LOWER(source) LIKE '%site.source.name%'
      OR LOWER(source) LIKE '%site_source_name%'
      OR LOWER(source) LIKE '%instagram%'
    THEN 'Meta Ads'

    WHEN (
        LOWER(source) LIKE '%google_ads%'
        AND (
            LOWER(campaign_medium) LIKE '%g%'
            OR LOWER(campaign_medium) LIKE '%search%'
            OR LOWER(campaign_medium) LIKE '%s%'
        )
    )
      OR LOWER(source) LIKE '%googleads%'
      OR LOWER(source) LIKE '%google adwords%'
    THEN 'Google Search'

    WHEN LOWER(source) LIKE '%pmax_upff%'
      OR (
        LOWER(source) LIKE '%google_ads%'
        AND LOWER(campaign_medium) LIKE '%pmax%'
      )
    THEN 'Google PMax'

    WHEN LOWER(source) LIKE '%youtube_upff%'
      OR (
        LOWER(source) LIKE '%google_ads%'
        AND (
            LOWER(campaign_medium) LIKE '%ytv%'
            OR LOWER(campaign_medium) LIKE '%x%'
        )
      )
    THEN 'Google Display'

    WHEN LOWER(source) LIKE '%google marketing platform%'
      OR LOWER(source) LIKE '%dv360_upff%'
    THEN 'Google Marketing Platform'

    WHEN LOWER(source) LIKE '%bing_ads%'
      OR LOWER(source) LIKE '%bing_upff%'
      OR LOWER(source) LIKE '%bing%'
      OR LOWER(source) LIKE '%bing ads%'
    THEN 'Bing Ads'

    WHEN LOWER(source) LIKE '%uptv-linear%'
      OR LOWER(source) LIKE '%linear-uptv%'
    THEN 'UPtv Linear'

    WHEN LOWER(source) LIKE '%uptv_movies_app%'
      OR LOWER(source) LIKE '%uptv-web%'
      OR LOWER(source) LIKE '%uptv-app%'
      OR LOWER(source) LIKE '%uptv%'
      OR LOWER(source) LIKE '%uptv.com%'
    THEN 'UPtv Digital'

    WHEN LOWER(source) LIKE '%aspire-linear%'
    THEN 'aspire TV Linear'

    WHEN LOWER(source) LIKE '%aspire.tv%'
    THEN 'aspire TV Digital'

    WHEN LOWER(source) LIKE '%zendesk%'
      OR LOWER(source) LIKE '%support%'
    THEN 'Customer Support'

    WHEN LOWER(source) LIKE '%google.com%'
      OR LOWER(source) LIKE '%android.gm%'
      OR LOWER(source) LIKE '%bing.com%'
      OR LOWER(source) LIKE '%yahoo.com%'
      OR LOWER(source) LIKE '%duckduckgo.com%'
    THEN 'Organic Search'

    WHEN LOWER(source) LIKE '%facebook.com%'
      OR LOWER(source) LIKE '%instagram.com%'
      OR LOWER(source) LIKE '%t.co%'
      OR LOWER(source) LIKE '%youtube.com%'
    THEN 'Organic Social'

    WHEN LOWER(source) LIKE '%seedtag%'
    THEN 'Seedtag'

    WHEN LOWER(source) LIKE '%cj_uptv%'
    THEN 'CJ'

    WHEN LOWER(source) LIKE '%unknown%'
    THEN 'Unknown'

    ELSE 'Others'
END AS marketing_platform
    from result)
    select *
    ,case
      WHEN LOWER(campaign_medium) LIKE '%facebook_mobile_feed%'
        OR LOWER(campaign_medium) LIKE '%facebook_desktop_feed%'
        OR LOWER(campaign_medium) LIKE '%instagram_feed%'
        OR LOWER(campaign_medium) LIKE '%instagram_stories%'
        OR LOWER(campaign_medium) LIKE '%instagram_reels%'
        OR LOWER(campaign_medium) LIKE '%instagram_profile_feed%'
        OR LOWER(campaign_medium) LIKE '%facebook_stories%'
        OR LOWER(campaign_medium) LIKE '%facebook_right_column%'
        OR LOWER(campaign_medium) LIKE '%facebook_marketplace%'
        OR LOWER(campaign_medium) LIKE '%facebook_instream_video%'
        OR (LOWER(campaign_medium) LIKE '%paid advertising%' AND marketing_platform = 'Meta Ads')
        OR marketing_platform = 'Organic Social'
        THEN 'Social Media'
      WHEN LOWER(campaign_medium) LIKE '%email%'
        OR LOWER(campaign_medium) LIKE '%eblast%'
        THEN 'Email Marketing'
      WHEN LOWER(campaign_medium) LIKE '%banner%'
        OR LOWER(campaign_medium) LIKE '%display%'
        OR (LOWER(campaign_medium) LIKE '%paid advertising%' AND marketing_platform = 'Google Marketing Platform')
        THEN 'Display Marketing'
      WHEN LOWER(campaign_medium) LIKE '%paid advertising%'
        OR LOWER(campaign_medium) LIKE '%search%'
        OR LOWER(campaign_medium) = 'g'
        THEN 'Search Engine Marketing'
      WHEN LOWER(campaign_medium) LIKE '%pmax%'
        THEN 'Cross-Platform Marketing'
      WHEN LOWER(campaign_medium) LIKE '%sms%'
        THEN 'SMS Marketing'
      WHEN LOWER(campaign_medium) LIKE '%ytv%'
        THEN 'Video Marketing'
      when marketing_platform = 'Organic Search'
        then 'Search Engine Optimization'
      ELSE 'Others/Unknown'
      END as marketing_channel
      from result2

      ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month, quarter, year]
    datatype: date

    sql: ${TABLE}.report_date ;;
  }
  dimension: source {
    type: string
    sql:${TABLE}.source;;
  }

  dimension: referrer {
    type: string
    sql:${TABLE}.referrer;;
  }

  dimension: marketing_platform {
    type: string
    sql:${TABLE}.marketing_platform;;
    }

  dimension: campaign_name {
    label: "Campaign Name"
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: marketing_channel {
    type: string
    sql:${TABLE}.marketing_channel;;
  }
  dimension: campaign_medium {
    label: "Campaign Medium"
    type: string
    sql: ${TABLE}.campaign_medium ;;
  }

  dimension: campaign_content {
    label: "Campaign Content"
    type: string
    sql: ${TABLE}.campaign_content ;;
  }

  dimension: campaign_term {
    label: "Campaign Term"
    type: string
    sql: ${TABLE}.campaign_term ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  measure: total_visits {
    label: "Total Visits"
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    value_format_name: decimal_0
  }
}
