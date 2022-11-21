view: hubspot_bogo_testing {
  derived_table: {
    sql: SELECT *
      FROM
      UNNEST(
        [
          struct('test1@bogo.com' as email, 1 as user_id, "test" as first_name, "1" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test2@bogo.com' as email, 2 as user_id, "test" as first_name, "2" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test3@bogo.com' as email, 3 as user_id, "test" as first_name, "3" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test4@bogo.com' as email, 4 as user_id, "test" as first_name, "4" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test5@bogo.com' as email, 5 as user_id, "test" as first_name, "5" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test6@bogo.com' as email, 6 as user_id, "test" as first_name, "6" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test7@bogo.com' as email, 7 as user_id, "test" as first_name, "7" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test8@bogo.com' as email, 8 as user_id, "test" as first_name, "8" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test9@bogo.com' as email, 9 as user_id, "test" as first_name, "9" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test10@bogo.com' as email, 10 as user_id, "test" as first_name, "10" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test11@bogo.com' as email, 11 as user_id, "test" as first_name, "11" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test12@bogo.com' as email, 12 as user_id, "test" as first_name, "12" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test13@bogo.com' as email, 13 as user_id, "test" as first_name, "13" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test14@bogo.com' as email, 14 as user_id, "test" as first_name, "14" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test15@bogo.com' as email, 15 as user_id, "test" as first_name, "15" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test16@bogo.com' as email, 16 as user_id, "test" as first_name, "16" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test17@bogo.com' as email, 17 as user_id, "test" as first_name, "17" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test18@bogo.com' as email, 18 as user_id, "test" as first_name, "18" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test19@bogo.com' as email, 19 as user_id, "test" as first_name, "19" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test20@bogo.com' as email, 20 as user_id, "test" as first_name, "20" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test21@bogo.com' as email, 21 as user_id, "test" as first_name, "21" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test22@bogo.com' as email, 22 as user_id, "test" as first_name, "22" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test23@bogo.com' as email, 23 as user_id, "test" as first_name, "23" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test24@bogo.com' as email, 24 as user_id, "test" as first_name, "24" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test25@bogo.com' as email, 25 as user_id, "test" as first_name, "25" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test26@bogo.com' as email, 26 as user_id, "test" as first_name, "26" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test27@bogo.com' as email, 27 as user_id, "test" as first_name, "27" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test28@bogo.com' as email, 28 as user_id, "test" as first_name, "28" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test29@bogo.com' as email, 29 as user_id, "test" as first_name, "29" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
          , struct('test30@bogo.com' as email, 30 as user_id, "test" as first_name, "30" as last_name, "enabled" as subscription_status, "yearly" as frequency, "customer.product.renewed" as topic, FALSE as moptin, "web" as platform, "upff" as vod_brand, TRUE as test_user)
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
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    primary_key: yes
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
