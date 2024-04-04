{{
    config(
        tags=['mart']
    )
}}


WITH

stg_customer AS (

    SELECT

        *

    FROM {{ ref('stg_customer') }}
    WHERE 
        region <> 'NUNAVUT'

),

stg_market AS (

    SELECT

        *

    FROM {{ ref('stg_market') }}

),

stg_order AS (

    SELECT

        *

    FROM {{ ref('stg_order') }}

),

stg_shipping AS (
    SELECT

        *

    FROM {{ ref('stg_shipping') }}
 
),

left_join_customer_market_order AS (
    SELECT
        c.*,
        m.*,
        o.*,
        s.Ship_Date
    FROM stg_customer c
    LEFT JOIN 
        stg_market m ON c.Cust_id = m.customer_id
    LEFT JOIN 
        stg_order o ON m.market_order_id = o.Ord_id
    LEFT JOIN 
        stg_shipping s ON s.Order_ID = o.Order_ID
),

res_shipping AS (
    SELECT *,
           DATEDIFF(day, Order_Date,Ship_Date) AS delayed_in_days,
           CASE 
               WHEN Order_Priority = 'HIGH' AND DATEDIFF(day, Order_Date,Ship_Date) > 3 THEN 'true'
               ELSE 'false'
           END AS support
    FROM left_join_customer_market_order
)


SELECT * FROM res_shipping

