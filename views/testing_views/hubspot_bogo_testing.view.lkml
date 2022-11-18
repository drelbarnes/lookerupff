view: hubspot_bogo_testing {
  derived_table: {
    sql: SELECT *
      FROM
      UNNEST(
        [
          struct('test1@bogo.com' as email, 1 as user_id, "test" as first_name, "1" as last_name, "free_trial" as subscription_status, "yearly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test2@bogo.com' as email, 2 as user_id, "test" as first_name, "2" as last_name, "free_trial" as subscription_status, "yearly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test3@bogo.com' as email, 3 as user_id, "test" as first_name, "3" as last_name, "free_trial" as subscription_status, "yearly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test4@bogo.com' as email, 4 as user_id, "test" as first_name, "4" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.updated" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test5@bogo.com' as email, 5 as user_id, "test" as first_name, "5" as last_name, "enabled" as subscription_status, "monthly" as frequency, "customer.product.updated" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test6@bogo.com' as email, 6 as user_id, "test" as first_name, "6" as last_name, "free_trial" as subscription_status, "monthly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test7@bogo.com' as email, 7 as user_id, "test" as first_name, "7" as last_name, "free_trial" as subscription_status, "yearly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test8@bogo.com' as email, 8 as user_id, "test" as first_name, "8" as last_name, "enabled" as subscription_status, "monthly" as frequency, "customer.product.updated" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test9@bogo.com' as email, 9 as user_id, "test" as first_name, "9" as last_name, "free_trial" as subscription_status, "yearly" as frequency, "customer.product.free_trial_created" as topic, FALSE as moptin, "ios" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test10@bogo.com' as email, 10 as user_id, "test" as first_name, "10" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.updated" as topic, FALSE as moptin, "roku" as platform, "upff" as vod_brand, TRUE as test_user)
          ]
      )
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: vod_brand {
    type: string
    sql: ${TABLE}.vod_brand ;;
  }

  dimension: test_user {
    type: yesno
    sql: ${TABLE}.test_user ;;
  }

  set: detail {
    fields: [
      email,
      user_id,
      first_name,
      last_name,
      subscription_status,
      frequency,
      topic,
      moptin,
      platform,
      vod_brand,
      test_user
    ]
  }
}
