{{ config
(
    materialized = 'table'
)
}}

SELECT
block_month,
l.name,
SUM(actualgascost) as user_gas_spend
FROM {{ ref('superchain_4337_userops') }} u
INNER JOIN {{ ref('superchain_4337_app_labels') }} l
  ON l.address = u.called_contract
  AND block_month < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
GROUP BY 1,2