{{
    config(
        tags=['basic', 'staging']
    )
}}


WITH

required_fields AS (


    SELECT 
        *

    FROM {{ source('online_shopping', 'shipping') }}

)


SELECT * FROM required_fields