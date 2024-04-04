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

seed_product AS (
    SELECT

        *
        
    FROM {{ ref('product') }}
),


final_res AS (
    SELECT
        m.*,
        PRODUCT_CATEGORY,
        PRODUCT_SUB_CATEGORY
    FROM stg_market m
    LEFT JOIN 
        seed_product p on p.PROD_ID = m.product_id
)

SELECT * from final_res
