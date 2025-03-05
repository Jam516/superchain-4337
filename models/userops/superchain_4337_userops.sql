{{ config
(
    materialized = 'incremental',
    unique_key = ['userop_hash','transaction_hash']
)
}}

SELECT
chain
, block_timestamp
, transaction_hash
, userop_hash
, from_address
, to_address
, bundler_address AS bundler
, userop_paymaster As paymaster
, CAST(useropevent_actualgascost AS numeric)/1e18 AS actualgascost
, case WHEN input != '0x' THEN 0
    else SAFE.PARSE_NUMERIC('0x' || SUBSTRING(userop_calldata, 75, 64))/1e18 
    end as value
FROM op-4337.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v2
WHERE is_from_sender = TRUE
AND userop_idx = 1 
{% if is_incremental() %}
AND dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
{% if not is_incremental() %}
dt >= DATE("2025-01-01")
{% endif %}


