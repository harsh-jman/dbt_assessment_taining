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
    FROM {{ ref('stg_market') }} m
    LEFT JOIN 
        stg_customer c ON c.Cust_id = m.customer_id
),



res_table AS (
    SELECT
        market_order_id,
        sum(profit) AS net_profit

    FROM stg_market
    GROUP BY market_order_id
)

-- ordering_desc AS (
--     SELECT
--         *
--     FROM res_table
--     ORDER BY net_profit DESC
-- )

SELECT * FROM res_table