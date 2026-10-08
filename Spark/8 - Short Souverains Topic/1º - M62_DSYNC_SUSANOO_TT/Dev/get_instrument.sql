SET serveroutput ON size unlimited;
SET DEFINE ON;
SET VERIFY OFF;
SET PAGESIZE 0;
SET LINESIZE 1500;
SET FEEDBACK OFF;
SET TRIMSPOOL ON
SET HEADING ON;
SET TERMOUT OFF;

SPOOL '&1';

select 	p.system_instrument_id||';'|| '&6'||';'|| p.instrument_tech_id||';'|| p.instrument_integration_id||';'||
        to_char(p.instr_start_date,'YYYYMMDD')||';'|| to_char(p.instr_settlement_date,'YYYYMMDD')||';'|| p.instr_price_currency||';'|| p.quotity||';'|| p.is_otc_instr||';'||
        p.issuer_dixit_code||';'|| p.issuer_boa_code||';'|| p.product_name_level_1||';'||p.product_name_level_2||';'||
        case 
      when p.product_name_level_2 = 'Bond' and p.key_factor_type is not null then 'real bond'
      when p.product_name_level_2 = 'Bond' and p.key_factor_type is null then 'technical'
      when p.product_name_level_2 = 'Bond_LNR' then 'Issued by BPCE'
	  else p.product_name_level_3
    end ||';'||
    p.key_factor||';'|| p.key_factor_type||';'|| to_char(p.instr_maturity_date,'YYYYMMDD')||';'|| p.jv||';'||p.content_description||';'||
	replace(p.NATIVE_XML_INSTR_DETAILS.extract('/Valeur/CodeISIN/text()').getStringVal(),';','<POINT_VIRGULE>') ||';'||
	replace(P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/ListeDonneesMarche/Prix/text()').GETSTRINGVAL(),';','<POINT_VIRGULE>') ||';'||';'|| OPTION_TYPE_CODE||';'|| to_char(to_date('&2','DD/MM/YYYY'),'YYYYMMDD')
	||';'||'EOD &3'
	||';'||replace(P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/CodeInstrument/text()').GETSTRINGVAL(),';','<POINT_VIRGULE>') 
	||';'||replace(P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/LibelleInstrument/text()').GETSTRINGVAL(),';','<POINT_VIRGULE>')
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Trade/Description/ExternalReference/text()').GETSTRINGVAL()
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Trade/IRG/AssetIrg/ListValue/Value/Ccy/text()').GETSTRINGVAL()
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Trade/IRG/AssetIrg/ListValue/Value/PV/text()').GETSTRINGVAL()
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/ListeDonneesMarche/TheoricalPrice/text()').GETSTRINGVAL() 
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/ListeDonneesMarche/CouponCouru/text()').GETSTRINGVAL() 
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/SeniorityLevel/text()').GETSTRINGVAL() 
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/Nominal/text()').GETSTRINGVAL() 
	||';'||P.NATIVE_XML_INSTR_DETAILS.EXTRACT('/Valeur/ListeGisement/TitreGisement/IdentifiantSousJacent/text()').GETSTRINGVAL() 
	from table(FA_GET_INSTRUMENTS ( CLOSING => 'EOD &3',
                  AsOf =>  TO_DATE('&2', 'DD/MM/YYYY'),
                  LabelLongName => '&4',
				  ContentDescriptionsIncluded => '&5'
				  ,WithXML => 'Y'
				  --,
                 --VersionLabel => &5
                  --WITHHIERARCHY => 'N'
			)) p
				 --where p.content_description in ('&6')
				 ;

SPOOL OFF;

quit
