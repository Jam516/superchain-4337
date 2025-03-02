{{ config
(
    materialized = 'table'
)
}}

WITH earliest_userop AS (
  SELECT 
  sender,
  block_month,
  l.name,
  ROW_NUMBER() OVER (
    PARTITION BY sender
    ORDER BY block_timestamp
  ) AS rn
  FROM {{ ref('superchain_4337_userops') }} u
  INNER JOIN {{ ref('superchain_4337_app_labels') }} l
    ON l.address = u.called_contract
    AND block_month < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
)

SELECT
block_month,
name,
COUNT(DISTINCT sender) AS onboarded_addresses
FROM earliest_userop
WHERE rn = 1
GROUP BY 1,2