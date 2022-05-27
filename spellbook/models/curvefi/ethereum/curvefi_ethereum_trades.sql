 {{
  config(
        alias='trades'
  )
}}

SELECT
    'ethereum' as blockchain,
    'Curve' AS project,
    '' AS version,
    evt_block_time AS block_time,
    buyer AS trader_a,
    NULL as trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN bought_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN sold_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','susd_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL as trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN bought_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN sold_id = 3 THEN '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','susd_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL as trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xF61718057901F84C4eEC4339EF8f0D86D2B45600'
        WHEN bought_id = 1 THEN '0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xF61718057901F84C4eEC4339EF8f0D86D2B45600'
        WHEN sold_id = 1 THEN '0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','susd_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL as trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xF61718057901F84C4eEC4339EF8f0D86D2B45600'
        WHEN bought_id = 1 THEN '0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xF61718057901F84C4eEC4339EF8f0D86D2B45600'
        WHEN sold_id = 1 THEN '0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','susd_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN bought_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN sold_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','compound_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL as trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN bought_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN sold_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','compound_v2_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','compound_v2_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN bought_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN sold_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','compound_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','compound_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN bought_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'
        WHEN sold_id = 1 THEN '0x39AA39c021dfbaE8faC545936693aC917d5E7563'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','usdt_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','usdt_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01'
        WHEN bought_id = 1 THEN '0xd6aD7a6750A7593E092a9B218d66C0A814a3436e'
        WHEN bought_id = 2 THEN '0x83f798e925BcD4017Eb265844FDDAbb448f1707D'
        WHEN bought_id = 3 THEN '0x73a052500105205d34Daf004eAb301916DA8190f'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01'
        WHEN sold_id = 1 THEN '0xd6aD7a6750A7593E092a9B218d66C0A814a3436e'
        WHEN sold_id = 2 THEN '0x83f798e925BcD4017Eb265844FDDAbb448f1707D'
        WHEN sold_id = 3 THEN '0x73a052500105205d34Daf004eAb301916DA8190f'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','y_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN bought_id = 3 THEN '0x0000000000085d4780B73119b644AE5ecd22b376'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN sold_id = 3 THEN '0x0000000000085d4780B73119b644AE5ecd22b376'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','y_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xC2cB1040220768554cf699b0d863A3cd4324ce32'
        WHEN bought_id = 1 THEN '0x26EA744E5B887E5205727f55dFBE8685e3b21951'
        WHEN bought_id = 2 THEN '0xE6354ed5bC4b393a5Aad09f21c46E101e692d447'
        WHEN bought_id = 3 THEN '0x04bC0Ab673d88aE9dbC9DA2380cB6B79C4BCa9aE'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xC2cB1040220768554cf699b0d863A3cd4324ce32'
        WHEN sold_id = 1 THEN '0x26EA744E5B887E5205727f55dFBE8685e3b21951'
        WHEN sold_id = 2 THEN '0xE6354ed5bC4b393a5Aad09f21c46E101e692d447'
        WHEN sold_id = 3 THEN '0x04bC0Ab673d88aE9dbC9DA2380cB6B79C4BCa9aE'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','busd_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN bought_id = 3 THEN '0x4Fabb145d64652a948d72533023f6E7A623C7C53'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN sold_id = 3 THEN '0x4Fabb145d64652a948d72533023f6E7A623C7C53'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','busd_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x99d1Fa417f94dcD62BfE781a1213c092a47041Bc'
        WHEN bought_id = 1 THEN '0x9777d7E2b60bB01759D0E2f8be2095df444cb07E'
        WHEN bought_id = 2 THEN '0x1bE5d71F2dA660BFdee8012dDc58D024448A0A59'
        WHEN bought_id = 3 THEN '0x8E870D67F660D95d5be530380D0eC0bd388289E1'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x99d1Fa417f94dcD62BfE781a1213c092a47041Bc'
        WHEN sold_id = 1 THEN '0x9777d7E2b60bB01759D0E2f8be2095df444cb07E'
        WHEN sold_id = 2 THEN '0x1bE5d71F2dA660BFdee8012dDc58D024448A0A59'
        WHEN sold_id = 3 THEN '0x8E870D67F660D95d5be530380D0eC0bd388289E1'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','pax_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN bought_id = 3 THEN '0x8E870D67F660D95d5be530380D0eC0bd388289E1'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
        WHEN sold_id = 3 THEN '0x8E870D67F660D95d5be530380D0eC0bd388289E1'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','pax_swap_evt_TokenExchangeUnderlying') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'
        WHEN bought_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'
        WHEN sold_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','ren_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'
        WHEN bought_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
        WHEN bought_id = 2 THEN '0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'
        WHEN sold_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
        WHEN sold_id = 2 THEN '0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','sbtc_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN bought_id = 0 THEN '0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'
        WHEN bought_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
    END as token_a_address,
    CASE
        --change address back to renBTC's, right now Dune only tracks WBTC price
        WHEN sold_id = 0 THEN '0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6'
        WHEN sold_id = 1 THEN '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','hbtc_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN bought_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN bought_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0x6B175474E89094C44Da98b954EedeAC495271d0F'
        WHEN sold_id = 1 THEN '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
        WHEN sold_id = 2 THEN '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','threepool_swap_evt_TokenExchange') }} 

UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        WHEN bought_id = 1 THEN '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        WHEN sold_id = 1 THEN '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','steth_swap_evt_TokenExchange') }} 


UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '2' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN bought_id = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN bought_id = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN sold_id = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN sold_id = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','tricrypto_swap_evt_TokenExchange') }} 


UNION

SELECT
    'ethereum' as blockchain,
    evt_block_time AS block_time,
    'Curve' AS project,
    '2' AS version,
    buyer AS trader_a,
    NULL AS trader_b,
    tokens_bought AS token_a_amount_raw,
    tokens_sold AS token_b_amount_raw,
    CASE
        WHEN bought_id = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN bought_id = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN bought_id = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    END as token_a_address,
    CASE
        WHEN sold_id = 0 THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN sold_id = 1 THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN sold_id = 2 THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    END as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    '' AS trace_address,
    evt_index
FROM {{ source('curvefi_ethereum','tricrypto2_swap_evt_TokenExchange') }}