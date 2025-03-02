{{ config
(
    materialized = 'table'
)
}}

SELECT
block_month,
l.name,
COUNT(op_hash) as num_ops
FROM op-4337.dbt_kofi.superchain_4337_userops u
INNER JOIN op-4337.dbt_kofi.superchain_4337_app_labels l
  ON l.address = u.called_contract
  AND block_month < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
GROUP BY 1,2