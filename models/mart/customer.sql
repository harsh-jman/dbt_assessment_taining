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


left_join_customer_market_order AS (
    SELECT
        *
    FROM stg_customer c
    LEFT JOIN stg_market m ON c.Cust_id = m.customer_id
    LEFT JOIN stg_order o ON m.market_order_id = o.Ord_id
),

res_min_max_count AS (
    SELECT 
        customer_id,
        MIN(Order_Date) AS first_ordered_date,
        MAX(Order_Date) AS most_recent_date,
        SUM(Sales) AS lifetime_ordered_value,
        COUNT(*) AS total_orders
    FROM 
        left_join_customer_market_order
    GROUP BY 
        customer_id
),

result AS (
    SELECT 
        *,
        DATEDIFF(day, first_ordered_date, most_recent_date) AS days_between_first_and_recent
    FROM 
        res_min_max_count
)

SELECT * FROM result

