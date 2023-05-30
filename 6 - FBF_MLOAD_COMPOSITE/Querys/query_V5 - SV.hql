SELECT 
    primary_table.alert_id,
    primary_table.alert_state,
    primary_table.alert_start,
    primary_table.alert_end,
    primary_table.is_currently_in_alert,
    primary_table.cod_raf,
    primary_table.code_parent,
    primary_table.code_indicator,
    primary_table.is_overage,
    primary_table.overage_start,
    primary_table.overage_end,
    primary_table.weight,
    primary_table.is_visible,
    primary_table.preco_metier,
    primary_table.preco_risk,
    primary_table.indicator_value,
    primary_table.date_last_financial_state,
    primary_table.financial_account,
    primary_table.bref_form_id,
    primary_table.nb_is_overage_start,
    primary_table.partition_key 
FROM (
        SELECT  --E PRECISO CRIAR VARIAVEL PYTHON COM TODAS AS COLUNAS EXCETO "overage_start". Depois, ter este case na query com uma , antes
            primary_table.alert_id,
            primary_table.alert_state,
            primary_table.alert_start,
            primary_table.alert_end,
            primary_table.is_currently_in_alert,
            primary_table.cod_raf,
            primary_table.code_parent,
            primary_table.code_indicator,
            primary_table.is_overage,
            primary_table.overage_end,
            primary_table.weight,
            primary_table.is_visible,
            primary_table.preco_metier,
            primary_table.preco_risk,
            primary_table.indicator_value,
            primary_table.date_last_financial_state,
            primary_table.financial_account,
            primary_table.bref_form_id,
            primary_table.nb_is_overage_start,
            primary_table.partition_key,
            CASE
                WHEN primary_table.is_overage_almost = 'CheckConditionIndicatorValue' AND primary_table.is_overage = 1 THEN overage_start_min
                ELSE primary_table.overage_start
            END overage_start  
        FROM ( 
                SELECT 
                    primary_table.alert_id,
        		    primary_table.alert_state,
        		    primary_table.alert_start,
        		    primary_table.alert_end,
        		    primary_table.is_currently_in_alert,
        		    primary_table.cod_raf,
        		    primary_table.code_parent,
        		    primary_table.code_indicator, 
                    primary_table.overage_start,
        		    primary_table.overage_end,
        		    primary_table.weight,
        		    primary_table.is_visible,
        		    primary_table.preco_metier,
        		    primary_table.preco_risk,
        		    primary_table.indicator_value,
        		    primary_table.date_last_financial_state,
        		    primary_table.financial_account,
        		    primary_table.bref_form_id,
        		    primary_table.partition_key, --NAO ESQUECER ANTES DO CASE NAO TEM VIRGULA
                    CASE 
                        WHEN primary_table.is_overage_almost = 'CheckConditionIndicatorValue' AND CheckConditionIndicatorValue.indicator_value_min = 0 THEN CAST(1 AS DOUBLE) 
                        WHEN primary_table.is_overage_almost = 'CheckConditionIndicatorValue' AND (CheckConditionIndicatorValue.indicator_value_min <> 0 OR CheckConditionIndicatorValue.indicator_value_min IS NULL) THEN CAST(0 AS DOUBLE) 
                        ELSE CAST(primary_table.is_overage_almost AS DOUBLE) 
                    END is_overage, 
                    primary_table.overage_start_min, -- NOVO CAMPO
                    primary_table.is_overage_almost, --Campo novo que vai ser usado para a query em cima
                    primary_table.nb_is_overage_start_final AS nb_is_overage_start
                FROM ( 
                        SELECT 
                            main2.*, 
                            PreviousCKP_FamDegNotExt.is_overage_last, 
                            PreviousCKP_FamDegNotExt.overage_start_min, -- NOVO CAMPO
                            CASE 
                                WHEN main2.code_indicator = 'FamDegNotExt' THEN 
                                    CASE 
                                        WHEN (main2.is_overage = 1 AND PreviousCKP_FamDegNotExt.is_overage_last = 1) THEN 1 
                                        
                                        WHEN (main2.is_overage = 1 AND PreviousCKP_FamDegNotExt.is_overage_last = 0) THEN 1 
                                        
                                        WHEN (main2.is_overage = 1 AND PreviousCKP_FamDegNotExt.is_overage_last IS NULL) THEN 1 
                                        
                                        WHEN (main2.is_overage = 0 AND PreviousCKP_FamDegNotExt.is_overage_last = 1) THEN 'CheckConditionIndicatorValue' 
                                        
                                        WHEN ((main2.is_overage = 0 AND PreviousCKP_FamDegNotExt.is_overage_last = 0)) OR (main2.is_overage = 0 AND PreviousCKP_FamDegNotExt.is_overage_last IS NULL) THEN 0 
                                        
                                        ELSE NULL 
                                    END 
                                ELSE main2.is_overage 
                            END is_overage_almost 
                        FROM ( 
                                SELECT 
                                    main.*, 
                                    CASE 
                                        WHEN main.code_indicator <> 'FamDegNotExt' THEN sum_overage.sum_overage 
                                        ELSE NULL 
                                    END nb_is_overage_start_final 
                                FROM ( 
                                        SELECT 
                                            	primary_table.alert_id,
                                                primary_table.alert_state,
                                                primary_table.alert_start,
                                                primary_table.alert_end,
                                                primary_table.is_currently_in_alert,
                                                primary_table.cod_raf,
                                                primary_table.code_parent,
                                                primary_table.code_indicator,
                                                primary_table.is_overage,
                                                primary_table.overage_start,
                                                primary_table.overage_end,
                                                primary_table.weight,
                                                primary_table.is_visible,
                                                primary_table.preco_metier,
                                                primary_table.preco_risk,
                                                primary_table.indicator_value,
                                                primary_table.date_last_financial_state,
                                                primary_table.financial_account,
                                                primary_table.bref_form_id,
                                                primary_table.nb_is_overage_start,
                                                primary_table.partition_key  
                                        FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator primary_table 
                                            INNER JOIN ( 
                                                            SELECT 
                                                                aux2.cod_raf, 
                                                                aux2.code_indicator, 
                                                                aux2.is_overage, 
                                                                MAX(aux2.partition_key) AS partition_key 
                                                            FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator aux2 
                                                                INNER JOIN ( 
                                                                            SELECT 
                                                                                cod_raf, 
                                                                                code_indicator, 
                                                                                max(is_overage) AS m_is_overage 
                                                                            FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                                                            WHERE partition_key LIKE '202210%0' 
                                                                            GROUP BY cod_raf, code_indicator 
                                                                            ) aux3 ON (aux2.cod_raf=aux3.cod_raf AND aux2.code_indicator=aux3.code_indicator AND aux2.is_overage=aux3.m_is_overage) 
                                                            WHERE aux2.partition_key LIKE '202210%0' AND aux2.is_overage=1 
                                                            GROUP BY aux2.cod_raf, aux2.code_indicator, aux2.is_overage 
                                                    ) aux1 ON (primary_table.cod_raf = aux1.cod_raf AND primary_table.code_indicator =aux1.code_indicator AND primary_table.is_overage =aux1.is_overage AND primary_table.partition_key =aux1.partition_key) 
                                        WHERE primary_table.partition_key LIKE '202210%0' AND primary_table.code_indicator IN ('FamDegNotExt', 'famDegNoteInt', 'NumCdsDegBrut', 'NumCdsDegCont') 
                                        
                                        UNION ALL 
                                        
                                        SELECT 
                                            	primary_table.alert_id,
                                                primary_table.alert_state,
                                                primary_table.alert_start,
                                                primary_table.alert_end,
                                                primary_table.is_currently_in_alert,
                                                primary_table.cod_raf,
                                                primary_table.code_parent,
                                                primary_table.code_indicator,
                                                primary_table.is_overage,
                                                primary_table.overage_start,
                                                primary_table.overage_end,
                                                primary_table.weight,
                                                primary_table.is_visible,
                                                primary_table.preco_metier,
                                                primary_table.preco_risk,
                                                primary_table.indicator_value,
                                                primary_table.date_last_financial_state,
                                                primary_table.financial_account,
                                                primary_table.bref_form_id,
                                                primary_table.nb_is_overage_start,
                                                primary_table.partition_key  
                                        FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator primary_table 
                                            INNER JOIN ( 
                                                            SELECT 
                                                                aux2.cod_raf, 
                                                                aux2.code_indicator, 
                                                                aux2.is_overage, 
                                                                MAX(aux2.partition_key) AS partition_key 
                                                            FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator aux2 
                                                                INNER JOIN ( 
                                                                                SELECT 
                                                                                    cod_raf, 
                                                                                    code_indicator, 
                                                                                    max(is_overage) AS m_is_overage 
                                                                                FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                                                                WHERE partition_key LIKE '202210%0' 
                                                                                GROUP BY cod_raf, code_indicator 
                                                                           ) aux3 ON (aux2.cod_raf=aux3.cod_raf AND aux2.code_indicator=aux3.code_indicator AND aux2.is_overage=aux3.m_is_overage) 
                                                                INNER JOIN ( 
                                                                                SELECT 
                                                                                    MAX(partition_key) AS partition_key 
                                                                                    FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                                                                    WHERE partition_key LIKE '202210%0' 
                                                                            ) aux4 ON (aux2.partition_key=aux4.partition_key) 
                                                            WHERE aux2.partition_key LIKE '202210%0' AND aux2.is_overage=0 
                                                            GROUP BY aux2.cod_raf, aux2.code_indicator, aux2.is_overage 
                                                        ) aux1 ON (primary_table.cod_raf = aux1.cod_raf AND primary_table.code_indicator =aux1.code_indicator AND primary_table.is_overage =aux1.is_overage AND primary_table.partition_key =aux1.partition_key) 
                                        WHERE primary_table.partition_key LIKE '202210%0' AND primary_table.code_indicator IN ('FamDegNotExt', 'famDegNoteInt', 'NumCdsDegBrut', 'NumCdsDegCont') 
                                        
                                        UNION ALL 
                                        
                                        SELECT 
                                            	primary_table.alert_id,
                                                primary_table.alert_state,
                                                primary_table.alert_start,
                                                primary_table.alert_end,
                                                primary_table.is_currently_in_alert,
                                                primary_table.cod_raf,
                                                primary_table.code_parent,
                                                primary_table.code_indicator,
                                                primary_table.is_overage,
                                                primary_table.overage_start,
                                                primary_table.overage_end,
                                                primary_table.weight,
                                                primary_table.is_visible,
                                                primary_table.preco_metier,
                                                primary_table.preco_risk,
                                                primary_table.indicator_value,
                                                primary_table.date_last_financial_state,
                                                primary_table.financial_account,
                                                primary_table.bref_form_id,
                                                primary_table.nb_is_overage_start,
                                                primary_table.partition_key  
                                        FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator primary_table 
                                        INNER JOIN ( 
                                                        SELECT 
                                                            MAX(aux2.partition_key) AS partition_key 
                                                            FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator aux2 
                                                        WHERE aux2.partition_key LIKE '202210%0'
                                                   ) aux1 ON (primary_table.partition_key =aux1.partition_key) 
                                        WHERE primary_table.partition_key LIKE '202210%0' AND primary_table.code_indicator NOT IN ('FamDegNotExt', 'famDegNoteInt', 'NumCdsDegBrut', 'NumCdsDegCont') 
                                     ) main 
                                        LEFT JOIN ( 
                                                    SELECT 
                                                        cod_raf, 
                                                        code_indicator, 
                                                        SUM(is_overage) AS sum_overage 
                                                    FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                                    WHERE partition_key LIKE '202210%0' 
                                                    GROUP BY cod_raf, code_indicator 
                                                  ) sum_overage ON (main.cod_raf=sum_overage.cod_raf AND main.code_indicator=sum_overage.code_indicator) 
                             ) main2 
                                LEFT JOIN ( 
                                            SELECT cod_raf, code_indicator, is_overage AS is_overage_last, overage_start AS overage_start_min
                                            FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                            WHERE partition_key = '202209300001' 
                                          ) PreviousCKP_FamDegNotExt ON (main2.cod_raf=PreviousCKP_FamDegNotExt.cod_raf AND main2.code_indicator=PreviousCKP_FamDegNotExt.code_indicator) 
                     ) primary_table 
                        LEFT JOIN ( 
                                    SELECT cod_raf, MIN(indicator_value) AS indicator_value_min 
                                    FROM fbf_credit_alert.tbl_cust_fbf_reporting_indicator 
                                    WHERE partition_key LIKE '202210%0' 
                                    AND code_parent = 'FamDegNotExt' 
                                    AND UPPER(code_indicator) NOT LIKE '%M12%' 
                                    AND indicator_value NOT IN ('False', 'True') 
                                    AND indicator_value IS NOT NULL 
                                    GROUP BY cod_raf 
                                  ) CheckConditionIndicatorValue ON primary_table.cod_raf=CheckConditionIndicatorValue.cod_raf 
        ) primary_table
    )primary_table