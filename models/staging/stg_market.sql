{{
    config(
        tags=['basic', 'staging']
    )
}}


WITH

required_fields AS (


    SELECT 
        Ord_id AS market_order_id, 
        Prod_id AS product_id, 
        Ship_id AS shipment_id, 
        Cust_id AS customer_id, 
        CAST(Sales AS DECIMAL(10,2)) AS Sales,
        CAST(Discount AS DECIMAL(5,2)) AS Discount,
        Order_Quantity, 
        CAST(Profit AS DECIMAL(10,2)) AS Profit,
        CAST(Shipping_Cost AS DECIMAL(10,2)) AS Shipping_Cost,
        CAST(Product_Base_Margin AS DECIMAL(5,2)) AS Product_Base_Margin

    FROM {{ source('online_shopping', 'market') }}

)


SELECT * FROM required_fields