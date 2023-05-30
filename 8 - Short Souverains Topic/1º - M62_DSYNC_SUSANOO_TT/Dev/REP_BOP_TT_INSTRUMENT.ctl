OPTIONS (SILENT=(HEADER, FEEDBACK))
LOAD DATA
CHARACTERSET UTF8
TRUNCATE
INTO TABLE REP_BOP_TT_INSTRUMENT
FIELDS  TERMINATED  BY ';'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
system_instrument_id        ,
perimeter_id,
instrument_tech_id,
instrument_integration_id,
instr_start_date,
instr_settlement_date,
instr_price_currency,
quotity,
is_otc_instr,
issuer_dixit_code,
issuer_boa_code,
product_name_level_1,
product_name_level_2,
product_name_level_3,
key_factor,
key_factor_type,
instr_maturity_date,
jv,
content_description,
cod_isin,
market_price,
lib_source,
option_type_code,
asofdate,
closing,
cod_instrument,
lib_instrument,
cod_external_ref,
Ccy,
PV,
MOT_THEORICAL_PRICE,
MOT_ACCRUED_PRICE,
COD_SENIORITY_LEVEL,
MOT_NOMINAL,
COD_UNDERLYING
)

