view: upff_page_events {
  derived_table: {
    sql: CREATE TEMP FUNCTION URLDECODE(url STRING) AS ((
        SELECT SAFE_CONVERT_BYTES_TO_STRING(
          ARRAY_TO_STRING(ARRAY_AGG(
              IF(STARTS_WITH(y, '%'), FROM_HEX(SUBSTR(y, 2)), CAST(y AS BYTES)) ORDER BY i
            ), b''))
        FROM UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}|[^%]+")) AS y WITH OFFSET AS i
      ));
      with site_pages as (
        select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as ott_user_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , coalesce(safe_cast(user_email as string), safe_cast(context_traits_email as string)) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Page Viewed" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from javascript_upff_home.pages
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , app_pages as (
        select
        *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as ott_user_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , coalesce(safe_cast(user_email as string), safe_cast(context_traits_email as string)) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Page Viewed" as event
          , safe_cast(platform as string) as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from javascript.pages
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , identifies as (
        select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as ott_user_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , coalesce(safe_cast(email as string), safe_cast(context_traits_email as string)) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Identify" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from javascript.identifies
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , order_completed as (
          select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as ott_user_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , coalesce(safe_cast(email as string), safe_cast(user_email as string), safe_cast(context_traits_email as string)) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Order Completed" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
        from javascript.order_completed
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , web_events as (
        select * from site_pages
        union all
        select * from app_pages
        union all
        select * from identifies
        union all
        select * from order_completed
      )
      , checkout_pages as (
      /* Checkout URLS
      UPFF - Monthly
      Step 1: https://subscribe.upentertainment.com/index.php/welcome/plans/upfaithandfamily OR https://subscribe.upentertainment.com/
      Step 2: https://subscribe.upentertainment.com/index.php/welcome/create_account/upfaithandfamily/monthly/oJ331lRuT2qq6ymFfa3K
      Step 3: https://subscribe.upentertainment.com/index.php/welcome/payment/upfaithandfamily/monthly/oJ331lRuT2qq6ymFfa3K
      Step 4: https://subscribe.upentertainment.com/index.php/welcome/up_sell/upfaithandfamily/monthly
      Step 5: https://subscribe.upentertainment.com/index.php/welcome/confirmation/upfaithandfamily/monthly
      UPFF - Yearly
      */
        select
        *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as customer_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(null as string) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(null as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Page Viewed" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from javascript_upentertainment_checkout.pages
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , checkout_identifies as (
        select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as customer_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(email as string) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(null as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(null as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Identify" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from javascript_upentertainment_checkout.identifies
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , checkout_started as (
          select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as customer_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(null as string) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(checkout_id as string) as checkout_id
          , safe_cast(null as string) as order_id
          , safe_cast(null as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Checkout Started" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
        from javascript_upentertainment_checkout.checkout_started
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , checkout_order_completed as (
          select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as customer_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(user_email as string) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(checkout_id as string) as checkout_id
          , safe_cast(order_id as string) as order_id
          , safe_cast(null as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Order Completed" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
        from javascript_upentertainment_checkout.order_completed
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , checkout_order_updated as (
          select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as customer_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(user_email as string) as email
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(checkout_id as string) as checkout_id
          , safe_cast(order_id as string) as order_id
          , safe_cast(null as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Order Updated" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , case when NET.REG_DOMAIN(safe_cast(context_page_url as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_url as string)) end AS domain
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), '\\?(.+)')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , case when NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) = "entertainment.com" then "upentertainment.com" else NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) end AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
        from javascript_upentertainment_checkout.order_updated
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        )
      )
      , checkout_events as (
        select * from checkout_pages
        union all
        select * from checkout_identifies
        union all
        select * from checkout_started
        union all
        select * from checkout_order_completed
        union all
        select * from checkout_order_updated
      )
      , upff_checkout_events as (
        select * from checkout_events where referrer not like "%gaithertvplus%" and referrer_domain not like "%gaithertvplus%"
      )
      , id_mapping_table as (
        SELECT a.customer_id, b.ott_user_id
        FROM customers.tg_middleware_abc9876_customers a
        left join customers.tg_middleware_abc9876_ott_users b
        on a.id = b.customer_id
      )
      , customer_id_mapping as (
        select
        id
        ,timestamp
        , anonymous_id
        , coalesce(c.customer_id, b.customer_id) as customer_id
        , safe_cast(coalesce(c.ott_user_id, b.ott_user_id) as string) as ott_user_id
        , email
        , ip_address
        , checkout_id
        , order_id
        , cross_domain_id
        , event
        , referrer
        , search
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , title
        , url
        , domain
        , path
        , user_agent
        , platform
        from web_events a
        left join ${chargebee_vimeo_ott_id_mapping.SQL_TABLE_NAME} b
        on a.ott_user_id = safe_cast(b.ott_user_id as string)
        left join ${chargebee_vimeo_ott_id_mapping.SQL_TABLE_NAME} c
        on a.ott_user_id = c.customer_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
      )
      , ott_user_id_mapping as (
        select
        -- a.*, safe_cast(b.ott_user_id as string) as ott_user_id
        id
        ,timestamp
        , anonymous_id
        , a.customer_id
        , safe_cast(b.ott_user_id as string) as ott_user_id
        , email
        , ip_address
        , checkout_id
        , order_id
        , cross_domain_id
        , event
        , referrer
        , search
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , title
        , url
        , domain
        , path
        , user_agent
        , platform
        from upff_checkout_events a
        left join ${chargebee_vimeo_ott_id_mapping.SQL_TABLE_NAME} b
        on a.customer_id = b.customer_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
      )
      , all_events as (
        select * from customer_id_mapping
        union all
        select * from ott_user_id_mapping
      )
      -- DATA CLEANING AND PROCESSING
      -- unique event_id creation
      , page_event_ids as (
        select
        to_hex(sha1(concat(id,safe_cast(timestamp as string)))) as event_id
        , *
        from all_events
      )
      , app_session_mapping_p0 as (
        select
        lag(timestamp,1) over (partition by ott_user_id order by timestamp) as last_event_0
        , *
        from page_event_ids
      )
      , app_session_mapping_p1 as (
        select *
        , case
          when unix_seconds(timestamp) - unix_seconds(last_event_0) >= (60 * 30) or last_event_0 is null
            then 1
          else 0
          end as is_session_start_0
        from app_session_mapping_p0
      )
      , anon_id_mapping_p0 as (
        select
        *,
        case when event = "Identify" and is_session_start_0 = 1 then anonymous_id
          else null
          end as anon_id_2
        from app_session_mapping_p1
      )
      , anon_id_mapping_p1 as (
        select *
        , sum(case when anon_id_2 is null then 0 else 1 end) over (partition by ott_user_id order by timestamp) as session_partition
        from anon_id_mapping_p0
        where ott_user_id is not null
      )
      , anon_id_mapping_p2 as (
        select *
        , first_value(anon_id_2) over (partition by ott_user_id, session_partition order by timestamp) as anon_id_alt
        from anon_id_mapping_p1
        where ott_user_id is not null
      )
      , anon_id_mapping_p3 as (
        select a.*, b.anon_id_alt
        from app_session_mapping_p1 a
        left join anon_id_mapping_p2 b
        on a.event_id = b.event_id
      )
      , anon_id_mapping_p4 as (
        select
        -- Event Time
        timestamp,
        -- Event Information
        event,
        event_id,
        -- User Identification
        coalesce(anon_id_alt, anonymous_id) as anonymous_id,
        anonymous_id as anonymous_id_raw,
        customer_id,
        ott_user_id,
        email,
        ip_address,
        cross_domain_id,
        user_agent,
        platform,
        -- Domain, URL, Search and Page Information
        domain,
        url,
        path,
        search,
        title,
        -- Referrer Information
        referrer,
        referrer_domain,
        -- Advertising Information
        ad_id,
        adset_id,
        campaign_id,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_term,
        utm_content
        from anon_id_mapping_p3
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
      )
      , anon_id_mapping_p5 as (
        select
        a.timestamp
        -- Event Information
        , a.event
        , a.event_id
        -- User Identification
        , a.anonymous_id as mapped_anonymous_id
        , b.anonymous_id as cross_domain_anonymous_id
        , a.anonymous_id_raw
        , case
        when a.anonymous_id = b.anonymous_id then a.anonymous_id
        when b.anonymous_id = a.anonymous_id_raw then a.anonymous_id
        when a.anonymous_id = a.anonymous_id_raw and a.anonymous_id != b.anonymous_id then b.anonymous_id
        when b.anonymous_id is null then a.anonymous_id
        else a.anonymous_id
        end as anonymous_id
        , a.customer_id
        , a.ott_user_id
        , a.email
        , a.ip_address
        , a.cross_domain_id
        , a.user_agent
        , a.platform
        -- Domain, URL, Search and Page Information
        , a.domain
        , a.url
        , a.path
        , a.search
        , a.title
        -- Referrer Information
        , a.referrer
        , a.referrer_domain
        -- Advertising Information
        , a.ad_id
        , a.adset_id
        , a.campaign_id
        , a.utm_source
        , a.utm_medium
        , a.utm_campaign
        , a.utm_term
        , a.utm_content
        from anon_id_mapping_p4 a
        left join (select anonymous_id, ip_address, user_agent, timestamp, domain, referrer_domain from all_events where event = "Page Viewed") b
        on a.ip_address = b.ip_address
        and a.user_agent = b.user_agent
        and abs(timestamp_diff(a.timestamp, b.timestamp, second)) <= 300
        and a.domain = b.referrer_domain
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29 order by timestamp
      )
      -- END OF BLOCK
      , session_mapping_p0 as (
        select *
        , row_number() over (order by timestamp) as event_number
        from anon_id_mapping_p5
      )
      , session_mapping_p1 as (
        select *
        , lag(timestamp,1) over (partition by anonymous_id order by event_number) as last_event
        , lag(utm_campaign,1) over (partition by anonymous_id order by event_number) as last_utm_campaign
        , lead(timestamp, 1) over (partition by anonymous_id order by event_number) as next_event
        from session_mapping_p0
      )
      , campaign_session_mapping_p1 as (
        select *
        , case
          when utm_campaign is not null and (ifnull(last_utm_campaign, '') != utm_campaign) then 1
          when last_event is null or unix_seconds(timestamp) - unix_seconds(last_event) >= (7 * 24 * 60 * 60) then 1
          when utm_campaign is null and (referrer_domain is not null and referrer_domain not in ("upfaithandfamily.com", "upentertainment.com")) then 1
          else 0
          end as is_session_start
        from session_mapping_p1
      )
      , campaign_session_mapping_p2 as (
        select *
        , lead(is_session_start,1) over (partition by anonymous_id order by event_number) as next_session
        from campaign_session_mapping_p1
      )
      , campaign_session_mapping_p3 as (
        select *
        , case
          when next_session = 1 then 1
          when next_session is null then 1
          else 0
          end as is_session_end
        , case
          when event = "Order Completed" then 1
          else 0
          end as is_conversion
        from campaign_session_mapping_p2
      )
      , time_session_mapping_p1 as (
        select *
        , case
            when unix_seconds(timestamp) - unix_seconds(last_event) >= (60 * 30) or last_event is null
            then 1
            else 0
            end as is_session_start
        , case
            when unix_seconds(next_event) - unix_seconds(timestamp) >= (60 * 30) or next_event is null
            then 1
            else 0
            end as is_session_end
        , case
            when event = "Order Completed" then 1
            else 0
            end as is_conversion
        from session_mapping_p1
        order by anonymous_id, timestamp
      )
      , session_ids_p0 as (
        select *
        , case when is_session_start = 1 then to_hex(sha1(concat(event_id,safe_cast(timestamp as string))))
          else null
          end as new_session_id
        from campaign_session_mapping_p3
      )
      , session_ids_p1 as (
        select *
        , sum(case when new_session_id is null then 0 else 1 end) over (partition by anonymous_id order by event_number) as session_partition
        from session_ids_p0
      )
      , session_ids_p2 as (
        select *
        , first_value(new_session_id) over (partition by anonymous_id, session_partition order by event_number) as session_id_alt
        from session_ids_p1
      )
      , session_ids_p3 as (
        select
        timestamp
        , anonymous_id
        , anonymous_id_raw
        , customer_id
        , ip_address
        , cross_domain_id
        , ott_user_id
        , email
        , event
        , event_id
        , event_number
        , session_id_alt as session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , domain
        , referrer
        , search
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , title
        , url
        , path
        , user_agent
        , platform
        from session_ids_p2
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
      )
      select * from session_ids_p3;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: event_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.event_id ;;
  }

  dimension: ott_user_id {
    type: string
    sql: ${TABLE}.ott_user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: anonymous_id_raw {
    type: string
    sql: ${TABLE}.anonymous_id_raw ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: cross_domain_id {
    type: string
    sql: ${TABLE}.cross_domain_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_number {
    type: number
    sql: ${TABLE}.event_number ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: domain {
    type: string
    sql: ${TABLE}.domain ;;
  }

  dimension: search {
    type: string
    sql: ${TABLE}.search ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: path {
    type: string
    sql: ${TABLE}.path ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: is_session_start {
    type: yesno
    sql: ${TABLE}.is_session_start = 1 ;;
  }

  dimension: is_session_end {
    type: yesno
    sql: ${TABLE}.is_session_end = 1 ;;
  }

  dimension: is_conversion {
    type: yesno
    sql: ${TABLE}.is_conversion = 1 ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: campaign_source {
    sql: CASE
      WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is null or ${TABLE}.referrer_domain = "upfaithandfamily.com/" then "unknown"
      WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is not null and ${TABLE}.referrer_domain != "upfaithandfamily.com/" then ${TABLE}.referrer_domain
      WHEN ${TABLE}.utm_source LIKE 'hs_email' then 'Internal'
      WHEN ${TABLE}.utm_source LIKE 'hs_automation' then 'Internal'
      WHEN ${TABLE}.utm_source LIKE '%site.source.name%' then 'Facebook Ads'
      WHEN ${TABLE}.utm_source LIKE '%site_source_name%' then 'Facebook Ads'
      WHEN ${TABLE}.utm_source = 'google_ads' then 'Google Ads'
      WHEN ${TABLE}.utm_source = 'GoogleAds' then 'Google Ads'
      WHEN ${TABLE}.utm_source = 'fb' then 'Facebook Ads'
      WHEN ${TABLE}.utm_source = 'facebook' then 'Facebook Ads'
      WHEN ${TABLE}.utm_source = 'ig' then 'Facebook Ads'
      WHEN ${TABLE}.utm_source = 'bing_ads' then 'Bing Ads'
      WHEN ${TABLE}.utm_source = 'an' then 'Facebook Ads'
      else ${TABLE}.utm_source
    END ;;
  }

  set: detail {
    fields: [
      ott_user_id,
      anonymous_id,
      ip_address,
      cross_domain_id,
      event,
      utm_content,
      utm_medium,
      utm_campaign,
      utm_source,
      utm_term,
      referrer,
      title,
      url,
      path,
      user_agent,
      session_id,
      is_session_start,
      is_session_end,
      is_conversion,
      platform,
      timestamp_time,
      event_number
    ]
  }
}
