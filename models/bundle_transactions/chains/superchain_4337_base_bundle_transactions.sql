{{ config(materialized='table') }}

SELECT
  DISTINCT `hash`
FROM
  {{ source('base_raw', 'base_transactions') }}
WHERE
  to_address IN ('0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789',
    '0x0576a174d229e3cfa37253523e645a78a0c91b57',
    '0x0f46c65c17aa6b4102046935f33301f0510b163a',
    '0x0000000071727de22e5e9d8baf0edac6f37da032')
  AND block_timestamp > TIMESTAMP("2024-09-01")
LIMIT
  10