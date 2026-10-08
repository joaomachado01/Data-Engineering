create or replace PACKAGE BODY PACK_REP_BATCH_RISQ_SOUV
as

FUNCTION EXTRACT_RISQ_SOUV_REPORT RETURN CUST_RISQ_SOUV_REPORT_LIST PIPELINED as
    
    liste CUST_RISQ_SOUV_REPORT_LIST;
    v_step varchar2(3) := '1';
    v_syst varchar2(30) := 'RISQUES_SOUVERAINS_REPORT';
    v_row CUST_RISQ_SOUV_REPORT := CUST_RISQ_SOUV_REPORT(
                                                            NULL, 
                                                            NULL,
                                                            NULL,
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL,
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL, 
                                                            NULL,
                                                            NULL,
                                                            NULL
                                                        );
    v_partition varchar2(12);

BEGIN   

    -- Sélection de la partition
    SELECT PACK_REP_BATCH.CALC_ARRETE('SUS')||'0000' INTO v_partition FROM DUAL;

    FOR i IN (  
                select 
                    FUTURE.lib_instrument AS FUT_LIBELLE,
                    FUTURE.cod_instrument_id_orig AS FUT_ID,
                    OBLIGATION.cod_instrument_id_orig AS FUT_ID_UND,
                    substr(rep_bop_position.cod_book,1,3) AS FUT_SECTION,
                    rep_bop_position.cod_book AS FUT_SOUS_SECTION,
                    FUTURE.mot_market_price*rep_bop_position.mot_quantity*rep_bop_position.NUM_QUOTITY AS FUT_MTM_EURO,
                    rep_bop_position.mot_quantity AS FUT_QUANT, 
                    CASE
                        WHEN (FUTURE.dat_maturity IS NULL) OR (FUTURE.dat_maturity = '') THEN ''
                        ELSE SUBSTR(FUTURE.dat_maturity,7,2)||'/'||SUBSTR(FUTURE.dat_maturity,5,2)||'/'||SUBSTR(FUTURE.dat_maturity,1,4)
                    END FUT_ECHEANCE,
                    1 AS FUT_NOMINAL,
                    FUTURE.cod_price_currency AS FUT_DEVISE,
                    FUTURE. MOT_THEORICAL_PRICE AS FUT_PRIX, 
                    OBLIGATION.lib_instrument AS BOND_LIBELLE,
                    OBLIGATION.MOT_NOMINAL AS BOND_NOMINAL,  
                    OBLIGATION. MOT_ACCRUED_PRICE AS BOND_COURU, 
                    OBLIGATION.cod_price_currency AS BOND_DEVISE,
                    OBLIGATION.MOT_THEORICAL_PRICE AS BOND_PRIX,
                    '% pied de coupon' AS BOND_PRIX_TYPE, 
                    CASE WHEN OBLIGATION.cod_key_factor IS NULL AND substr(OBLIGATION.cod_instrument_id_orig,1,2)='DE' THEN 'BCALLEMAGN' ELSE OBLIGATION.cod_key_factor END AS BOND_EMETTEUR,
                    OBLIGATION.COD_SENIORITY_LEVEL AS BOND_SENIORITY, 
                    NVL(ROUND(SENSI_TABLE.SENSI_POSITION* rep_bop_position.mot_quantity,5),0) AS AGGREGATED_RHO_EURO
                FROM 
                    rep_bop_instrument OBLIGATION,
                    rep_bop_instrument FUTURE,
                    rep_bop_position,
                    (select POSITION_COUNT.cod_book,POSITION_COUNT.cod_instrument_id_orig,CASE WHEN TOTAL_POSITION = 0 THEN 0 ELSE MOT_SENSI_EUR/TOTAL_POSITION END SENSI_POSITION from 
                    (select COD_BOOK,COD_INSTRUMENT,MOT_SENSI_EUR from REP_SUS_SENSI where key_partition = v_partition) SENSI, 
                    (
                    select FUTURE.cod_instrument_id_orig,rep_bop_position.cod_book,sum(rep_bop_position.mot_quantity) TOTAL_POSITION
                    FROM rep_bop_position,rep_bop_instrument FUTURE WHERE rep_bop_position.key_partition = v_partition and 
                    rep_bop_position.key_partition=FUTURE.key_partition
                    and rep_bop_position.Cod_Instrument_Tech=FUTURE.Cod_Instrument_Tech
                    and FUTURE.LIB_PRODUCT_NAME_LEVEL_1  in ('FUTURE')
                    GROUP BY rep_bop_position.key_partition,rep_bop_position.Cod_Instrument_Tech,FUTURE.cod_instrument_id_orig,rep_bop_position.cod_book) POSITION_COUNT
                    WHERE SENSI.COD_BOOK= POSITION_COUNT.cod_book
                    AND SENSI.COD_INSTRUMENT=POSITION_COUNT.cod_instrument_id_orig) SENSI_TABLE
                where OBLIGATION.key_partition = v_partition 
                and rep_bop_position.key_partition=OBLIGATION.key_partition
                and rep_bop_position.key_partition=FUTURE.key_partition
                and rep_bop_position.Cod_Instrument_Tech=FUTURE.Cod_Instrument_Tech 
                and FUTURE.cod_price_currency='EUR'
                and (OBLIGATION.cod_key_factor  IN  ( 'REPAUTRICH','ROYBELG','BULGARIE','REPCHYPRE','TCHEQUE','RODANEMARK','ESTONIE','EFSFLUX','BEILX','EUROCOMLU','REPFINLAND','CADESPA','KFWFT','REPGRECE','ROHUNGARY','ROISLANDE','ROIRELAND','REPITALIE','LETTONIE','LITUANIE','BEILX','MALTE','ROPAYSBAS','NORVEGE','REPOLOGNE','ROPORTU','ROROUMANIE','SLOVAQUIE','ROFSLOV','ROYESPAGNE','ROYSUESK','ROYUNI','LBERLIN','HESSEN','HAMBURG','LRHEINLAND','LBADEWURT','LSAARLAND','LSHOLSTEIN','LTHURINGE','LNORDRHEIN','LFREISTAAT','LBRANDEBOU','LNIEDERSAC','LBREME','LSAXE','LSAXEANHAL','LMECKLENBU','-','WALLONNE','COMFRBELG','BRUXCAP','TRESOR','CFRETR','REPALLEMAG','ROISLANDE'  )
                    OR substr(OBLIGATION.cod_instrument_id_orig,1,2)='DE')
                
                and OBLIGATION.LIB_PRODUCT_NAME_LEVEL_1  in ('BOND','OBLIGATION','MUST')
                -- Point à vérifier and OBLIGATION.LIB_PRODUCT_NAME_LEVEL_1  in ('OBLIGATION')
                and FUTURE.LIB_PRODUCT_NAME_LEVEL_1  in  ('FUTURE','OPTION','IOPTION','SOPTION')
                AND OBLIGATION.cod_instrument_id_orig LIKE '%'||FUTURE.cod_instrument_id_orig||'%' 
                AND FUTURE.dat_maturity > to_char(sysdate,'YYYYMMDD') 
                AND rep_bop_position.cod_book= SENSI_TABLE.cod_book(+)
                AND FUTURE.cod_instrument_id_orig = SENSI_TABLE.cod_instrument_id_orig(+)
    )
    LOOP
        v_row.FUT_LIBELLE := i.FUT_LIBELLE;
        v_row.FUT_ID := i.FUT_ID;
        v_row.FUT_ID_UND := i.FUT_ID_UND;
        v_row.FUT_SECTION := i.FUT_SECTION;
        v_row.FUT_SOUS_SECTION := i.FUT_SOUS_SECTION;
        v_row.FUT_MTM_EURO := i.FUT_MTM_EURO;
        v_row.FUT_QUANT := i.FUT_QUANT;
        v_row.FUT_ECHEANCE := i.FUT_ECHEANCE;
        v_row.FUT_NOMINAL := i.FUT_NOMINAL;
        v_row.FUT_DEVISE := i.FUT_DEVISE;
        v_row.FUT_PRIX := i.FUT_PRIX;
        v_row.BOND_LIBELLE := i.BOND_LIBELLE;
        v_row.BOND_NOMINAL := i.BOND_NOMINAL;
        v_row.BOND_COURU := i.BOND_COURU;
        v_row.BOND_DEVISE := i.BOND_DEVISE;
        v_row.BOND_PRIX := i.BOND_PRIX;
        v_row.BOND_PRIX_TYPE := i.BOND_PRIX_TYPE;
        v_row.BOND_EMETTEUR := i.BOND_EMETTEUR;
        v_row.BOND_SENIORITY := i.BOND_SENIORITY;
        v_row.AGGREGATED_RHO_EURO := i.AGGREGATED_RHO_EURO;
        pipe ROW(v_row);
    END LOOP;

  RETURN;

EXCEPTION
    WHEN OTHERS THEN
    PACK_REP_BATCH.PISTE_AUDIT('A','SEND_RISQUES_SOUVERAINS_REPORT',null,v_step,v_syst);
    COMMIT;

END EXTRACT_RISQ_SOUV_REPORT; 

END PACK_REP_BATCH_RISQ_SOUV;