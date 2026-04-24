view: visits {
  derived_table: {
    sql:

      SELECT
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
          when campaign_source is null and (referrer is null or referrer in ('upfaithandfamily.com/', 'upfaithandfamily.com', 'vhx.tv'))
      then 'unknown'
      when campaign_source is null and (referrer is not null and referrer not in ('upfaithandfamily.com/', 'upfaithandfamily.com', 'vhx.tv'))
      then referrer
      else campaign_source
      END AS source
      FROM javascript_upff_home.pages
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

  dimension: marketing_platform {
    sql: CASE
    WHEN LOWER(${source}) = 'hs_email'
    or LOWER(${source}) = 'hs_automation'
    or LOWER(${source}) = 'hubspot_upff'
    or LOWER(${source}) = 'hubspot_uptv'
    or LOWER(${source}) = 'hubspot_gtv'
    then 'HubSpot'
    WHEN LOWER(${source}) = 'fb'
    or LOWER(${source}) = 'facebook'
    or LOWER(${source}) = 'ig'
    or LOWER(${source}) = 'an'
    or LOWER(${source}) LIKE '%site.source.name%'
    or LOWER(${source}) LIKE '%site_source_name%'
    or LOWER(${source}) = 'instagram'
    then 'Meta Ads'
    WHEN (
    LOWER(${source}) = 'google_ads'
    and (LOWER(${TABLE}.campaign_medium) = 'g' or LOWER(${TABLE}.campaign_medium) = 'search' or LOWER(${TABLE}.campaign_medium) = 's')
    )
    or LOWER(${source}) = 'googleads'
    or LOWER(${source}) = 'google adwords'
    then 'Google Search'
    WHEN LOWER(${source}) = 'pmax_upff'
    or (
    LOWER(${source}) = 'google_ads'
    and LOWER(${TABLE}.campaign_medium) = 'pmax'
    )
    then 'Google PMax'
    WHEN LOWER(${source}) = 'youtube_upff'
    or (
    LOWER(${source}) = 'google_ads'
    and (LOWER(${TABLE}.campaign_medium) = 'ytv' or LOWER(${TABLE}.campaign_medium) = 'x')
    )
    then 'Google Display'
    WHEN LOWER(${source}) = 'google marketing platform'
    or LOWER(${source}) = 'dv360_upff'
    then 'Google Marketing Platform'
    WHEN LOWER(${source}) = 'bing_ads'
    or LOWER(${source}) = 'bing_upff'
    or LOWER(${source}) = 'bing'
    or LOWER(${source}) = 'bing ads'
    then 'Bing Ads'
    WHEN LOWER(${source}) = 'uptv-linear'
    or LOWER(${source}) = 'linear-uptv'
    then 'UPtv Linear'
    WHEN LOWER(${source}) = 'uptv_movies_app'
    or LOWER(${source}) = 'uptv-web'
    or LOWER(${source}) = 'uptv-app'
    or LOWER(${source}) = 'uptv'
    or LOWER(${source}) = 'uptv.com'
    then 'UPtv Digital'
    WHEN LOWER(${source}) = 'aspire-linear'
    then 'aspire TV Linear'
    WHEN LOWER(${source}) = 'aspire.tv'
    then 'aspire TV Digital'
    WHEN LOWER(${source}) = 'zendesk'
    or LOWER(${source}) = 'support'
    then 'Customer Support'
    WHEN LOWER(${source}) = 'google.com'
    or LOWER(${source}) = 'android.gm'
    or LOWER(${source}) = 'bing.com'
    or LOWER(${source}) = 'yahoo.com'
    or LOWER(${source}) = 'duckduckgo.com'
    then 'Organic Search'
    WHEN LOWER(${source}) = 'facebook.com'
    or LOWER(${source}) = 'instagram.com'
    or LOWER(${source}) = 't.co'
    or LOWER(${source}) = 'youtube.com'
    then 'Organic Social'

    WHEN LOWER(${source}) = 'seedtag'
    then 'Seedtag'
    WHEN LOWER(${source}) = 'cj_uptv'
    then 'CJ'
    WHEN LOWER(${source}) = 'unknown'
    then 'Unknown'
    ELSE 'Others'
    END;;}

  dimension: campaign_name {
    label: "Campaign Name"
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: marketing_channel {
    sql:
      case
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%facebook_mobile_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_desktop_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_stories%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_reels%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_profile_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_stories%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_right_column%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_marketplace%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_instream_video%'
        OR (LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%' AND ${marketing_platform} = "Meta Ads")
        OR ${marketing_platform} = "Organic Social"
        THEN 'Social Media'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%email%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%eblast%'
        THEN 'Email Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%banner%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%display%'
        OR (LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%' AND ${marketing_platform} = "Google Marketing Platform")
        THEN 'Display Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%search%'
        OR LOWER(${TABLE}.utm_medium) = "g"
        THEN 'Search Engine Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%pmax%'
        THEN 'Cross-Platform Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%sms%'
        THEN 'SMS Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%ytv%'
        THEN 'Video Marketing'
      when ${marketing_platform} = 'Organic Search'
        then 'Search Engine Optimization'
      -- when ${TABLE}.utm_medium = '' then 'Website'
      -- when ${TABLE}.utm_medium = '' then 'Content Marketing'

      -- when ${TABLE}.utm_medium = '' then 'Affiliate Marketing'
      -- when ${TABLE}.utm_medium = '' then 'Influencer Marketing'
      -- when ${TABLE}.utm_medium = '' then 'TV Advertising'
      -- when ${TABLE}.utm_medium = '' then 'Mobile App'
      ELSE 'Others/Unknown'
      END ;;
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
