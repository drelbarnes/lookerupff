  view: radiant_reporting {
  derived_table: {
    sql: CREATE TEMP FUNCTION URLDECODE(url STRING) AS ((
        SELECT SAFE_CONVERT_BYTES_TO_STRING(
          ARRAY_TO_STRING(ARRAY_AGG(
              IF(STARTS_WITH(y, '%'), FROM_HEX(SUBSTR(y, 2)), CAST(y AS BYTES)) ORDER BY i
            ), b''))
        FROM UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}|[^%]+")) AS y WITH OFFSET AS i
      ));
      with checkout_pages AS (
        /* Checkout URLS
        UPFF - Monthly
        Step 1: https://subscribe.upentertainment.com/index.php/welcome/plans/upfaithandfamily OR https://subscribe.upentertainment.com/
        Step 2: https://subscribe.upentertainment.com/index.php/welcome/create_account/upfaithandfamily/monthly/oJ331lRuT2qq6ymFfa3K
        Step 3: https://subscribe.upentertainment.com/index.php/welcome/payment/upfaithandfamily/monthly/oJ331lRuT2qq6ymFfa3K
        Step 4: https://subscribe.upentertainment.com/index.php/welcome/up_sell/upfaithandfamily/monthly
        Step 5: https://subscribe.upentertainment.com/index.php/welcome/confirmation/upfaithandfamily/monthly
        UPFF - Yearly
        */
        SELECT
          *,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id,
          -- Adding fields for Radiant
          REPLACE(REGEXP_EXTRACT(search, '(?i)rsid=([^&]+)'), '+', ' ') AS rsid,
          REPLACE(REGEXP_EXTRACT(search, '(?i)track1=([^&]+)'), '+', ' ') AS track1
        FROM (
          SELECT
            id,
            timestamp,
            SAFE_CAST(user_id AS STRING) AS customer_id,
            SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
            REPLACE(REGEXP_EXTRACT(URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')), '(?i)email=([^&]+)'), '+', ' ') AS email,
            SAFE_CAST(context_ip AS STRING) AS ip_address,
            SAFE_CAST(NULL AS STRING) AS checkout_id,
            SAFE_CAST(NULL AS STRING) AS order_id,
            SAFE_CAST(NULL AS STRING) AS cross_domain_id,
            SAFE_CAST(context_user_agent AS STRING) AS user_agent,
            "Page Viewed" AS event,
            "web" AS platform,
            SAFE_CAST(context_page_url AS STRING) AS url,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) END AS domain,
            URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')) AS search,
            SAFE_CAST(context_page_referrer AS STRING) AS referrer,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) END AS referrer_domain,
            SAFE_CAST(context_page_title AS STRING) AS title,
            SAFE_CAST(context_page_path AS STRING) AS path
          FROM javascript_upentertainment_checkout.pages
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      ),
      checkout_identifies AS (
        SELECT *,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id,
          -- Adding fields for Radiant
          REPLACE(REGEXP_EXTRACT(search, '(?i)rsid=([^&]+)'), '+', ' ') AS rsid,
          REPLACE(REGEXP_EXTRACT(search, '(?i)track1=([^&]+)'), '+', ' ') AS track1
        FROM (
          SELECT
            id,
            timestamp,
            SAFE_CAST(user_id AS STRING) AS customer_id,
            SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
            SAFE_CAST(email AS STRING) AS email,
            SAFE_CAST(context_ip AS STRING) AS ip_address,
            SAFE_CAST(NULL AS STRING) AS checkout_id,
            SAFE_CAST(NULL AS STRING) AS order_id,
            SAFE_CAST(NULL AS STRING) AS cross_domain_id,
            SAFE_CAST(context_user_agent AS STRING) AS user_agent,
            "Identify" AS event,
            "web" AS platform,
            SAFE_CAST(context_page_url AS STRING) AS url,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) END AS domain,
            URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')) AS search,
            SAFE_CAST(context_page_referrer AS STRING) AS referrer,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) END AS referrer_domain,
            SAFE_CAST(context_page_title AS STRING) AS title,
            SAFE_CAST(context_page_path AS STRING) AS path
          FROM javascript_upentertainment_checkout.identifies
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      ),
      checkout_started AS (
        SELECT *,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id,
          -- Adding fields for Radiant
          REPLACE(REGEXP_EXTRACT(search, '(?i)rsid=([^&]+)'), '+', ' ') AS rsid,
          REPLACE(REGEXP_EXTRACT(search, '(?i)track1=([^&]+)'), '+', ' ') AS track1
        FROM (
          SELECT
            id,
            timestamp,
            SAFE_CAST(user_id AS STRING) AS customer_id,
            SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
            SAFE_CAST(NULL AS STRING) AS email,
            SAFE_CAST(context_ip AS STRING) AS ip_address,
            SAFE_CAST(checkout_id AS STRING) AS checkout_id,
            SAFE_CAST(NULL AS STRING) AS order_id,
            SAFE_CAST(NULL AS STRING) AS cross_domain_id,
            SAFE_CAST(context_user_agent AS STRING) AS user_agent,
            "Checkout Started" AS event,
            "web" AS platform,
            SAFE_CAST(context_page_url AS STRING) AS url,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) END AS domain,
            URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')) AS search,
            SAFE_CAST(context_page_referrer AS STRING) AS referrer,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) END AS referrer_domain,
            SAFE_CAST(context_page_title AS STRING) AS title,
            SAFE_CAST(context_page_path AS STRING) AS path
          FROM javascript_upentertainment_checkout.checkout_started
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      ),
      checkout_order_completed AS (
        SELECT *,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id,
          -- Adding fields for Radiant
          REPLACE(REGEXP_EXTRACT(search, '(?i)rsid=([^&]+)'), '+', ' ') AS rsid,
          REPLACE(REGEXP_EXTRACT(search, '(?i)track1=([^&]+)'), '+', ' ') AS track1
        FROM (
          SELECT
            id,
            timestamp,
            SAFE_CAST(user_id AS STRING) AS customer_id,
            SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
            SAFE_CAST(user_email AS STRING) AS email,
            SAFE_CAST(context_ip AS STRING) AS ip_address,
            SAFE_CAST(checkout_id AS STRING) AS checkout_id,
            SAFE_CAST(order_id AS STRING) AS order_id,
            SAFE_CAST(NULL AS STRING) AS cross_domain_id,
            SAFE_CAST(context_user_agent AS STRING) AS user_agent,
            "Order Completed" AS event,
            "web" AS platform,
            SAFE_CAST(context_page_url AS STRING) AS url,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) END AS domain,
            URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')) AS search,
            SAFE_CAST(context_page_referrer AS STRING) AS referrer,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) END AS referrer_domain,
            SAFE_CAST(context_page_title AS STRING) AS title,
            SAFE_CAST(context_page_path AS STRING) AS path
          FROM javascript_upentertainment_checkout.order_completed
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      ),
      checkout_order_updated AS (
        SELECT *,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content,
          REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id,
          COALESCE(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id,
          -- Adding fields for Radiant
          REPLACE(REGEXP_EXTRACT(search, '(?i)rsid=([^&]+)'), '+', ' ') AS rsid,
          REPLACE(REGEXP_EXTRACT(search, '(?i)track1=([^&]+)'), '+', ' ') AS track1
        FROM (
          SELECT
            id,
            timestamp,
            SAFE_CAST(user_id AS STRING) AS customer_id,
            SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
            SAFE_CAST(user_email AS STRING) AS email,
            SAFE_CAST(context_ip AS STRING) AS ip_address,
            SAFE_CAST(checkout_id AS STRING) AS checkout_id,
            SAFE_CAST(order_id AS STRING) AS order_id,
            SAFE_CAST(NULL AS STRING) AS cross_domain_id,
            SAFE_CAST(context_user_agent AS STRING) AS user_agent,
            "Order Updated" AS event,
            "web" AS platform,
            SAFE_CAST(context_page_url AS STRING) AS url,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_url AS STRING)) END AS domain,
            URLDECODE(REGEXP_EXTRACT(SAFE_CAST(context_page_url AS STRING), '\\?(.+)')) AS search,
            SAFE_CAST(context_page_referrer AS STRING) AS referrer,
            CASE WHEN NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) = "entertainment.com" THEN "upentertainment.com" ELSE NET.REG_DOMAIN(SAFE_CAST(context_page_referrer AS STRING)) END AS referrer_domain,
            SAFE_CAST(context_page_title AS STRING) AS title,
            SAFE_CAST(context_page_path AS STRING) AS path
          FROM javascript_upentertainment_checkout.order_updated
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      ),
      radiant_leads AS (
        SELECT
          timestamp,
          email,
          referrer,
          utm_campaign,
          utm_source,
          utm_medium,
          utm_content,
          utm_term,
          rsid,
          track1
        FROM checkout_pages
        WHERE email IS NOT NULL AND rsid IS NOT NULL
        ORDER BY timestamp DESC
      )
      SELECT
        a.timestamp,
        a.email,
        a.referrer,
        a.utm_campaign,
        a.utm_source,
        a.utm_medium,
        a.utm_content,
        a.utm_term,
        a.rsid,
        a.track1
      FROM radiant_leads a
      INNER JOIN checkout_order_completed b ON a.email = b.email AND a.timestamp < b.timestamp
    ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

    dimension_group: timestamp {
      type: time
      timeframes: [raw, time, date, week, month, quarter, year]
      sql: ${TABLE}.timestamp ;;
      label: "Event Timestamp"
      description: "The timestamp when the event occurred."
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
      label: "Email"
      description: "Email"
    }

    dimension: email_hash {
      type: string
      sql: SHA256(${email}) ;;
      label: "Email Hash"
      description: "SHA256 hash of the user's email address."
    }

    dimension: referrer {
      type: string
      sql: ${TABLE}.referrer ;;
      label: "Referrer URL"
      description: "The URL of the page that referred the user."
    }

    dimension: utm_campaign {
      type: string
      sql: ${TABLE}.utm_campaign ;;
      label: "UTM Campaign"
      description: "The UTM campaign parameter from the URL."
    }

    dimension: utm_source {
      type: string
      sql: ${TABLE}.utm_source ;;
      label: "UTM Source"
      description: "The UTM source parameter from the URL."
    }

    dimension: utm_medium {
      type: string
      sql: ${TABLE}.utm_medium ;;
      label: "UTM Medium"
      description: "The UTM medium parameter from the URL."
    }

    dimension: utm_content {
      type: string
      sql: ${TABLE}.utm_content ;;
      label: "UTM Content"
      description: "The UTM content parameter from the URL."
    }

    dimension: utm_term {
      type: string
      sql: ${TABLE}.utm_term ;;
      label: "UTM Term"
      description: "The UTM term parameter from the URL."
    }

    dimension: rsid {
      type: string
      sql: ${TABLE}.rsid ;;
      label: "RSID"
      description: "RSID parameter for Radiant."
    }

    dimension: track1 {
      type: string
      sql: ${TABLE}.track1 ;;
      label: "Track1"
      description: "Track1 parameter for Radiant."
    }
  }
