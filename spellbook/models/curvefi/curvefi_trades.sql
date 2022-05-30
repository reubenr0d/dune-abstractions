{{ config(
        alias='trades'
        )
}}

SELECT blockchain, project, version, block_time, trader_a, trader_b, 
       token_a_amount_raw, token_b_amount_raw, token_a_address, token_b_address, tx_hash, trace_address, evt_index FROM 
(SELECT blockchain, project, version, block_time, trader_a, trader_b, 
       token_a_amount_raw, token_b_amount_raw, token_a_address, token_b_address, tx_hash, trace_address, evt_index
 FROM {{ ref('curvefi_ethereum_view_trades') }} ) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where block_time > (select max(block_time) from {{ this }})
{% endif %} 