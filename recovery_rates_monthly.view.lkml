view: recovery_rates_monthly {
  derived_table: {
    sql: WITH /*** shows recover rates of customers who were in topic charged failed one week prior to being renewed  ***/

            a AS /*1*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-06-23' AND '2021-06-30'*/
                ((( status_date ) >= ((DATEADD(week,-13, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-13, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            aa AS (SELECT * FROM a WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            aaa AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM aa
            INNER JOIN http_api.purchase_event pe
            ON aa.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-06-30' AND '2021-07-07'*/
            ((( pe.status_date ) >= ((DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            aaaa AS
            (select
            (select date((DATEADD(week,-13, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from aaa) as recovered,
            (select count(*) from aa) as failed
            ),

            b AS /*2*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-06-30' AND '2021-07-07'*/
                ((( status_date ) >= ((DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            bb AS (SELECT * FROM b WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            bbb AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM bb
            INNER JOIN http_api.purchase_event pe
            ON bb.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-07-07' AND '2021-07-14'*/
            ((( pe.status_date ) >= ((DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            bbbb AS
            (select
            (select date((DATEADD(week,-12, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from bbb) as recovered,
            (select count(*) from bb) as failed
            ),

            c AS /*3*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-07-07' AND '2021-07-14'*/
                ((( status_date ) >= ((DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            cc AS (SELECT * FROM c WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            ccc AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM cc
            INNER JOIN http_api.purchase_event pe
            ON cc.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-07-14' AND '2021-07-21'*/
            ((( pe.status_date ) >= ((DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            cccc AS
            (select
            (select date((DATEADD(week,-11, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from ccc) as recovered,
            (select count(*) from cc) as failed
            ),

            d AS /*4*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-07-14' AND '2021-07-21'*/
                ((( status_date ) >= ((DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            dd AS (SELECT * FROM d WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            ddd AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM dd
            INNER JOIN http_api.purchase_event pe
            ON dd.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-07-21' AND '2021-07-28'*/
            ((( pe.status_date ) >= ((DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            dddd AS
            (select
            (select date((DATEADD(week,-10, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from ddd) as recovered,
            (select count(*) from dd) as failed
            ),

            e AS /*5*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-07-21' AND '2021-07-28'*/
                ((( status_date ) >= ((DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            ee AS (SELECT * FROM e WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            eee AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM ee
            INNER JOIN http_api.purchase_event pe
            ON ee.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-07-28' AND '2021-08-04'*/
            ((( pe.status_date ) >= ((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            eeee AS
            (select
            (select date((DATEADD(week,-9, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from eee) as recovered,
            (select count(*) from ee) as failed
            ),

            f AS /*6*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-07-28' AND '2021-08-04'*/
                ((( status_date ) >= ((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            ff AS (SELECT * FROM f WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            fff AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM ff
            INNER JOIN http_api.purchase_event pe
            ON ff.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-08-04' AND '2021-08-11'*/
            ((( pe.status_date ) >= ((DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            ffff AS
            (select
            (select date((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from fff) as recovered,
            (select count(*) from ff) as failed
            ),

            g AS /*7*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-08-04' AND '2021-08-11'*/
                ((( status_date ) >= ((DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            gg AS (SELECT * FROM g WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            ggg AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM gg
            INNER JOIN http_api.purchase_event pe
            ON gg.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-08-11' AND '2021-08-18'*/
            ((( pe.status_date ) >= ((DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            gggg AS
            (select
            (select date((DATEADD(week,-7, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from ggg) as recovered,
            (select count(*) from gg) as failed
            ),

            h AS /*8*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-08-11' AND '2021-08-18'*/
                ((( status_date ) >= ((DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            hh AS (SELECT * FROM h WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            hhh AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM hh
            INNER JOIN http_api.purchase_event pe
            ON hh.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-08-18' AND '2021-08-25'*/
            ((( pe.status_date ) >= ((DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            hhhh AS
            (select
            (select date((DATEADD(week,-6, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from hhh) as recovered,
            (select count(*) from hh) as failed
            ),

            i AS /*9*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-08-18' AND '2021-08-25'*/
                ((( status_date ) >= ((DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            ii AS (SELECT * FROM i WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            iii AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM ii
            INNER JOIN http_api.purchase_event pe
            ON ii.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-08-25' AND '2021-09-01'*/
            ((( pe.status_date ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            iiii AS
            (select
            (select date((DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from iii) as recovered,
            (select count(*) from ii) as failed
            ),

            j AS /*10*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-08-25' AND '2021-09-01'*/
                ((( status_date ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            jj AS (SELECT * FROM j WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            jjj AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM jj
            INNER JOIN http_api.purchase_event pe
            ON jj.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-09-01' AND '2021-09-08'*/
            ((( pe.status_date ) >= ((DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            jjjj AS
            (select
            (select date((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from jjj) as recovered,
            (select count(*) from jj) as failed
            ),

            k AS /*11*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type /*6*/
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-09-01' AND '2021-09-08'*/
                ((( status_date ) >= ((DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6
            ),

            kk AS (SELECT * FROM k WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            kkk AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM kk
            INNER JOIN http_api.purchase_event pe
            ON kk.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-09-08' AND '2021-09-15'*/
                ((( pe.status_date ) >= ((DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            kkkk AS
            (select
            (select date((DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from kkk) as recovered,
            (select count(*) from kk) as failed
            ),

            l AS /*12*/
            (
                SELECT
                    user_id, /*1*/
                    email, /*2*/
                CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
                    platform, /*4*/
                    max(topic) AS topic, /*5*/
                    CASE
                    WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
                    WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
                    END as customer_type, /*6*/
                    status_date
                FROM http_api.purchase_event
                WHERE /*DATE("status_date") between '2021-09-01' AND '2021-09-08'*/
                ((( status_date ) >= ((DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( status_date ) < ((DATEADD(week,1, DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
                GROUP BY 1,2,3,4,6,7
            ),

            ll AS (SELECT * FROM l WHERE topic IN ('customer.product.charge_failed') AND customer_type = 'Paid' and platform = 'web'),

            lll AS
            (
            SELECT
                pe.user_id,
                pe.topic
            FROM ll
            INNER JOIN http_api.purchase_event pe
            ON ll.user_id = pe.user_id
            WHERE /*DATE(pe.status_date) between '2021-09-08' AND '2021-09-15'*/
                ((( pe.status_date ) >= ((DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( pe.status_date ) < ((DATEADD(week,1, DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) ))) ))
            AND pe.topic = 'customer.product.renewed'
            ),

            llll AS
            (select
            (select date((DATEADD(week,-2, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as failed_week,
            (select date((DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) ) as recovered_week,
            (select count(*) from lll) as recovered,
            (select count(*) from ll) as failed
            ),

            z AS
            (
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from aaaa
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from bbbb
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from cccc
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from dddd
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from eeee
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from ffff
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from gggg
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from hhhh
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from iiii
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from jjjj
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from kkkk
            union all
            select *, round(cast((recovered*100.0/failed) as decimal(5,2)), 5) as rate from llll
            )

            select * from z
             ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: failed_week {
    type: date
    sql: ${TABLE}.failed_week ;;
  }

  dimension: recovered_week {
    type: date
    sql: ${TABLE}.recovered_week ;;
  }

  dimension: recovered {
    type: number
    sql: ${TABLE}.recovered ;;
  }

  dimension: failed {
    type: number
    sql: ${TABLE}.failed ;;
  }

  dimension: rate {
    type: number
    sql: ${TABLE}.rate ;;
  }

  set: detail {
    fields: [failed_week, recovered_week, recovered, failed, rate]
  }
}
