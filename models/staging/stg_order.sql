{{
    config(
        tags=['basic', 'staging']
    )
}}


WITH

required_fields AS (


    SELECT 
        *

    FROM {{ source('online_shopping', 'order') }}

)


SELECT * FROM required_fields