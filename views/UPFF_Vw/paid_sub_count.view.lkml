view: paid_sub_count {
  derived_table: {
    sql:
    with sub_count as (
      SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY sub_count_report_date_date
                ORDER BY received_at DESC NULLS LAST
            ) AS rn
        FROM looker.upff_v2_paid_sub_count
        ) AS ranked
      WHERE rn = 1
  ),
  sub_count2 as (
      SELECT
        sub_count_report_date_date as report_date
        ,'monthly' as billing_period
        ,ios_monthly as ios
        ,tvos_monthly as tvos
        ,roku_monthly as roku
        ,amazon_fire_tablet_monthly as amazon_fire_tablet
        ,amazon_fire_tv_monthly as amazon_fire_tv
        ,web_monthly as web
        ,vizio_monthly as vizio_tv
        ,android_monthly as android
        ,android_tv_monthly as android_tv
        FROM sub_count

      UNION ALL
      SELECT
        sub_count_report_date_date as report_date
        ,'yearly' as billing_period
        ,ios_yearly as ios
        ,tvos_yearly as tvos
        ,roku_yearly as roku
        ,amazon_fire_tablet_yearly as amazon_fire_tablet
        ,amazon_fire_tv_yearly as amazon_fire_tv
        ,web_yearly as web
        ,vizio_yearly as vizio_tv
        ,android_yearly as android
        ,android_tv_yearly as android_tv
      FROM sub_count)

    SELECT report_date, billing_period, 'ios' AS platform, ios AS user_count
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'tvos', tvos
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'roku', roku
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'amazon_fire_tablet', amazon_fire_tablet
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'amazon_fire_tv', amazon_fire_tv
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'web', web
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'vizio_tv', vizio_tv
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'android', android
    FROM sub_count2

    UNION ALL
    SELECT report_date, billing_period, 'android_tv', android_tv
    FROM sub_count2
;;
  }





}
