{{ config
(
    materialized = 'incremental',
    unique_key = ['op_hash','tx_hash']
)
}}

WITH base AS (    
    SELECT 
        op.chain
        , op.userop_sender AS sender
        , op.userop_paymaster AS paymaster
        , op.userop_hash AS op_hash
        , CAST(op.useropevent_actualgascost AS NUMERIC)/1e18 AS actualgascost
        , op.transaction_hash
        , op.block_timestamp
        , op.trace_address
        , tx.from_address AS bundler
        , op.userop_calldata AS executecall
    FROM op-4337.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v1 op
    INNER JOIN op-4337.optimism_superchain_raw_onchain_data.transactions tx 
        ON op.transaction_hash = tx.hash
        AND op.chain_id = tx.chain_id
        AND op.is_innerhandleop = true
    {% if is_incremental() %}
    WHERE op.dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND tx.dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
    {% if not is_incremental() %}
    WHERE op.dt >= DATE("2025-01-01")
    AND tx.dt >= DATE("2025-01-01")
    {% endif %}
)

, joined AS (
    SELECT
        b.*
        , t.to_address
        , t.input
        , t.trace_address
        , row_number() over (partition by b.sender, b.trace_address, b.transaction_hash order by t.trace_address asc) as first_call
    FROM base b
    INNER JOIN op-4337.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v1 t 
        ON b.transaction_hash = t.transaction_hash
        AND b.sender = t.from_address
        AND t.is_innerhandleop_subtrace = true
        AND split(b.trace_address, ',')[OFFSET(0)] = split(t.trace_address, ',')[OFFSET(0)] 
        AND ARRAY_LENGTH(split(t.trace_address, ',')) > 3
        AND t.call_type != 'delegatecall'
    {% if is_incremental() %}
    WHERE t.dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.dt >= DATE("2025-01-01")
    {% endif %}            
)

SELECT
chain
, block_timestamp
, DATE_TRUNC(TIMESTAMP_SECONDS(block_timestamp), DAY) AS block_date
, DATE_TRUNC(TIMESTAMP_SECONDS(block_timestamp), MONTH) AS block_month
, transaction_hash
, op_hash
, sender
, bundler
, paymaster
, case 
    WHEN input != '0x' THEN TO_ADDRESS
    WHEN (input = '0x' AND SAFE.PARSE_NUMERIC('0x' || SUBSTRING(executeCall, 75, 64)) /1e18 > 0) THEN 'native_transfer'
    else 'empty_call' 
    end as called_contract
, case 
    WHEN input != '0x' THEN LEFT(input,10)
    WHEN (input = '0x' AND SAFE.PARSE_NUMERIC('0x' || SUBSTRING(executeCall, 75, 64)) /1e18 > 0) THEN 'native_transfer'
    else 'empty_call' end as function_called
, actualgascost
, case WHEN input != '0x' THEN 0
    else SAFE.PARSE_NUMERIC('0x' || SUBSTRING(executeCall, 75, 64))/1e18 
    end as value
FROM joined
WHERE first_call = 1 