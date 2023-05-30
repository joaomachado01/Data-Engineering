SELECT 
		rule1.cod_raf,
		rule1.id_group,
		CASE
            WHEN (rule1.wl_level = 3 AND rule2_case1.rule2_id_group_has_only_1_raf IS NOT NULL) THEN rule1.cod_raf
            WHEN (rule1.wl_level <> 3 AND rule2_case1.rule2_id_group_has_only_1_raf IS NOT NULL) THEN NULL
            WHEN rule2_case2_1raf_with_level3.cd_raf_tete IS NOT NULL THEN rule2_case2_1raf_with_level3.cd_raf
            WHEN rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020.cd_raf_tete IS NOT NULL THEN rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020.cd_raf
            WHEN rule2_case2_manyrafs_with_level3_final_after_31_12_2020.cd_raf_tete IS NOT NULL THEN rule2_case2_manyrafs_with_level3_final_after_31_12_2020.cd_raf
            ELSE NULL
        END cod_raf_default,
		rule1.wl_level,
		CASE
            WHEN (rule1.wl_level = 3 AND rule2_case1.rule2_id_group_has_only_1_raf IS NOT NULL) THEN rule1.dt_default
            WHEN (rule1.wl_level <> 3 AND rule2_case1.rule2_id_group_has_only_1_raf IS NOT NULL) THEN NULL
            WHEN rule2_case2_1raf_with_level3.cd_raf_tete IS NOT NULL THEN rule2_case2_1raf_with_level3.default_status_date
            WHEN rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020.cd_raf_tete IS NOT NULL THEN rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020.default_status_date
            WHEN rule2_case2_manyrafs_with_level3_final_after_31_12_2020.cd_raf_tete IS NOT NULL THEN rule2_case2_manyrafs_with_level3_final_after_31_12_2020.default_status_date
            ELSE NULL
        END dt_default,
		rule1.FLG_CTP_DOUTEUX,
        cast(to_date(from_unixtime(unix_timestamp(substr(rule1.key_partition,1,8), 'yyyyMMdd'))) as date) AS reporting_date
FROM (
        SELECT 
        		base.key_partition,
        		base.cod_raf,
        		base.id_group,
        		base.wl_level,
        		base.dt_default,
        		max_level2.FLG_CTP_DOUTEUX
        FROM (
                SELECT 
                    ref.cd_raf AS cod_raf,
                    ref.cd_raf_tete AS id_group,
                    ref.key_partition,
                    watch.current_wl_level AS wl_level,
                    watch.default_status_date AS dt_default
                FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                    LEFT JOIN
                             (
                                SELECT *
                                FROM be4_ndod.tbl_global_watch
                                WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                             ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                WHERE ref.key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                --AND ref.cd_raf_tete = '0001580' --PARA TIRAR, APENAS PARA TESTE -> valor = 0, >2 raf
                --AND ref.cd_raf_tete = '0003973' --PARA TIRAR, APENAS PARA TESTE -> valor = 1, 1 raf apenas
                --AND ref.cd_raf_tete = '0000388' --PARA TIRAR, APENAS PARA TESTE -> valor = 0, 1 raf apenas
                --AND ref.cd_raf_tete = '1060789' --PARA TIARAR, rule2_case2_1raf_with_level3
                --AND ref.cd_raf_tete = '0547496' --PARA TIARAR, rule2_case2_1raf_with_level3
                --AND ref.cd_raf_tete = '0572114' --PARA TIARAR, rule2_case2_1raf_with_level3
             ) base LEFT JOIN 
                             (
                                SELECT 
                                        *,
                                        CASE
                                            WHEN level_ = 3 THEN 1
                                            ELSE 0
                                        END FLG_CTP_DOUTEUX 
                                FROM (
                                        SELECT 
                                            ref.cd_raf_tete,
                                            MAX(watch.current_wl_level) AS level_
                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                            LEFT JOIN
                                                     (
                                                        SELECT *
                                                        FROM be4_ndod.tbl_global_watch
                                                        WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                     ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                        WHERE ref.key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                        GROUP BY ref.cd_raf_tete
                                    )max_level
        
                             ) max_level2 ON base.id_group=max_level2.cd_raf_tete
        --WHERE max_level2.FLG_CTP_DOUTEUX = 1
        --WHERE base.id_group = '0003973'
    ) rule1 LEFT JOIN
                     (
                        SELECT DISTINCT rule2_id_group_has_only_1_raf
                        FROM (
                        		SELECT 
                        		    cd_raf_tete AS rule2_id_group_has_only_1_raf,
                        		    count(*) AS count_
                        		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                        		WHERE key_partition='202205310000'
                        		GROUP BY cd_raf_tete
                        	) a
                        WHERE a.count_ = 1
                     ) rule2_case1 ON rule1.id_group=rule2_case1.rule2_id_group_has_only_1_raf
            LEFT JOIN
                     (
                        SELECT 
                                ref.cd_raf_tete,
                                ref.cd_raf,
                                watch.current_wl_level,
                                watch.default_status_date
                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                            LEFT JOIN
                                     (
                                        SELECT *
                                        FROM be4_ndod.tbl_global_watch
                                        WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                     ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                        WHERE ref.key_partition='202205310000' 
                        AND ref.cd_raf_tete IN 
                                              (
                                                SELECT *
                                                FROM (
                                                        SELECT DISTINCT rule2_id_group_has_more_then_1_raf
                                                        FROM (
                                                        		SELECT 
                                                        		    cd_raf_tete AS rule2_id_group_has_more_then_1_raf,
                                                        		    count(*) AS count_
                                                        		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                                                        		WHERE key_partition='202205310000'
                                                        		GROUP BY cd_raf_tete
                                                        	) a
                                                        WHERE a.count_ > 1
                                                    ) rule2_id_group_has_more_then_1_raf 
                                              ) -- Im getting only the groups that has more then 1 raf
                        AND ref.cd_raf_tete IN
                                              (
                                                SELECT DISTINCT rule2_id_group_has_only_1_raf_with_level_3
                                                FROM (
                                                        SELECT 
                                                            ref.cd_raf_tete AS rule2_id_group_has_only_1_raf_with_level_3,
                                                            watch.current_wl_level,
                                                            COUNT(*) AS count_
                                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                        LEFT JOIN
                                                                 (
                                                                    SELECT *
                                                                    FROM be4_ndod.tbl_global_watch
                                                                    WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                 ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                        WHERE ref.key_partition='202205310000' 
                                                        AND watch.current_wl_level = 3
                                                        GROUP BY ref.cd_raf_tete, watch.current_wl_level
                                                    ) rule2_id_group_has_only_1_raf_with_level_3
                                                WHERE rule2_id_group_has_only_1_raf_with_level_3.count_ = 1
                                              ) -- Im getting only the groups that has only 1 raf with wl = 3
                        --AND ref.cd_raf_tete = '1060789'
                        AND watch.current_wl_level = 3 -- Thias was necessary to get onlty one line per id_group (since only 1 raf has level 3)
                     )rule2_case2_1raf_with_level3 ON rule1.id_group=rule2_case2_1raf_with_level3.cd_raf_tete
            LEFT JOIN 
                     (
                     --___TESTES - Lesser standard date
                        SELECT 
                                DISTINCT
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1_final.cd_raf_tete,
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1_final.cd_raf,
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1_final.default_status_date
                        FROM (
                                SELECT 
                                        rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1.*,
                                        ROW_NUMBER() OVER(PARTITION BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1.cd_raf_tete ORDER BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1.cd_raf DESC) AS row_
                                FROM (
                                        SELECT 
                                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_almostrank1.*,
                                                RANK() OVER(PARTITION BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_almostrank1.cd_raf_tete ORDER BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_almostrank1.mt_expo_global_ DESC, rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_almostrank1.default_status_date ASC) AS get_max_expo_rank
                                        FROM (
                                                SELECT 
                                                        rule2_case2_manyrafs_with_level3.*,
                                                        digital.mt_expo_global,
                                                        CASE
                                                            WHEN digital.mt_expo_global IS NULL THEN 0
                                                            ELSE digital.mt_expo_global
                                                        END mt_expo_global_
                                                FROM (
                                                        SELECT 
                                                                ref.cd_raf_tete,
                                                                ref.cd_raf,
                                                                watch.current_wl_level,
                                                                watch.default_status_date,
                                                                ref.key_partition
                                                                --ref.cd_raf_tete,
                                                                --MIN(watch.default_status_date) AS date_
                                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                            LEFT JOIN
                                                                     (
                                                                        SELECT *
                                                                        FROM be4_ndod.tbl_global_watch
                                                                        WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                     ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                        WHERE ref.key_partition='202205310000' 
                                                        AND ref.cd_raf_tete IN 
                                                                              (
                                                                                SELECT *
                                                                                FROM (
                                                                                        SELECT DISTINCT rule2_id_group_has_more_then_1_raf
                                                                                        FROM (
                                                                                        		SELECT 
                                                                                        		    cd_raf_tete AS rule2_id_group_has_more_then_1_raf,
                                                                                        		    count(*) AS count_
                                                                                        		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                                                                                        		WHERE key_partition='202205310000'
                                                                                        		GROUP BY cd_raf_tete
                                                                                        	) a
                                                                                        WHERE a.count_ > 1
                                                                                    ) rule2_id_group_has_more_then_1_raf 
                                                                              ) -- Im getting only the groups that has more then 1 raf
                                                        AND ref.cd_raf_tete IN
                                                                              (
                                                                                SELECT DISTINCT rule2_id_group_has_only_1_raf_with_level_3
                                                                                FROM (
                                                                                        SELECT 
                                                                                            ref.cd_raf_tete AS rule2_id_group_has_only_1_raf_with_level_3,
                                                                                            watch.current_wl_level,
                                                                                            COUNT(*) AS count_
                                                                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                        LEFT JOIN
                                                                                                 (
                                                                                                    SELECT *
                                                                                                    FROM be4_ndod.tbl_global_watch
                                                                                                    WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                 ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                        WHERE ref.key_partition='202205310000' 
                                                                                        AND watch.current_wl_level = 3
                                                                                        GROUP BY ref.cd_raf_tete, watch.current_wl_level
                                                                                    ) rule2_id_group_has_only_1_raf_with_level_3
                                                                                WHERE rule2_id_group_has_only_1_raf_with_level_3.count_ > 1
                                                                              ) -- Im getting only the groups that has many rafs with wl = 3
                                                        --AND ref.cd_raf_tete = '0440312'
                                                        --AND ref.cd_raf_tete = '0491597'
                                                        --AND ref.cd_raf_tete = '0665914' -- PARA TIRAR 
                                                        --AND ref.cd_raf_tete = '0008062' -- PARA TIRAR
                                                        AND watch.current_wl_level = 3
                                                        --GROUP BY ref.cd_raf_tete
                                                    ) rule2_case2_manyrafs_with_level3 
                                                            LEFT JOIN 
                                                                     (
                                                                        SELECT 
                                                                            cd_raf, 
                                                                            CASE
                                                                                WHEN mt_expo_global IS NULL THEN 0
                                                                                ELSE mt_expo_global
                                                                            END mt_expo_global
                                                                        FROM gbn_credit_risk_reporting.tbl_t_dtm_digital
                                                                        WHERE key_partition = '202205310001' --HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP [A TABELA AQUI E DIFERENTE DAS OUTRAS, AQUI A KP VAI SER UMA DO TIPO 0001]
                                                                     ) digital ON rule2_case2_manyrafs_with_level3.cd_raf=digital.cd_raf
                                                WHERE rule2_case2_manyrafs_with_level3.cd_raf_tete IN 
                                                                                                     (
                                                                                                     
                                                                                                        SELECT 
                                                                                                            DISTINCT rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes.cd_raf_tete
                                                                                                        FROM (
                                                                                                                SELECT 
                                                                                                                        rule2_case2_manyrafs_with_level3_case_min_date.*,
                                                                                                                        CASE
                                                                                                                            WHEN rule2_case2_manyrafs_with_level3_case_min_date.min_ < '2020-12-31' THEN 'yes'
                                                                                                                            ELSE 'no'
                                                                                                                        END lesser_then_standard_date
                                                                                                                FROM (
                                                                                                                        SELECT 
                                                                                                                                rule2_case2_manyrafs_with_level3.cd_raf_tete,
                                                                                                                                MIN(rule2_case2_manyrafs_with_level3.default_status_date) AS min_
                                                                                                                        FROM (
                                                                                                                                SELECT 
                                                                                                                                        ref.cd_raf_tete,
                                                                                                                                        ref.cd_raf,
                                                                                                                                        watch.current_wl_level,
                                                                                                                                        watch.default_status_date,
                                                                                                                                        ref.key_partition
                                                                                                                                        --ref.cd_raf_tete,
                                                                                                                                        --MIN(watch.default_status_date) AS date_
                                                                                                                                FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                                                                    LEFT JOIN
                                                                                                                                             (
                                                                                                                                                SELECT *
                                                                                                                                                FROM be4_ndod.tbl_global_watch
                                                                                                                                                WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                                                             ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                                                                WHERE ref.key_partition='202205310000' 
                                                                                                                                AND ref.cd_raf_tete IN 
                                                                                                                                                      (
                                                                                                                                                        SELECT *
                                                                                                                                                        FROM (
                                                                                                                                                                SELECT DISTINCT rule2_id_group_has_more_then_1_raf
                                                                                                                                                                FROM (
                                                                                                                                                                		SELECT 
                                                                                                                                                                		    cd_raf_tete AS rule2_id_group_has_more_then_1_raf,
                                                                                                                                                                		    count(*) AS count_
                                                                                                                                                                		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                                                                                                                                                                		WHERE key_partition='202205310000'
                                                                                                                                                                		GROUP BY cd_raf_tete
                                                                                                                                                                	) a
                                                                                                                                                                WHERE a.count_ > 1
                                                                                                                                                            ) rule2_id_group_has_more_then_1_raf 
                                                                                                                                                      ) -- Im getting only the groups that has more then 1 raf
                                                                                                                                AND ref.cd_raf_tete IN
                                                                                                                                                      (
                                                                                                                                                        SELECT DISTINCT rule2_id_group_has_only_1_raf_with_level_3
                                                                                                                                                        FROM (
                                                                                                                                                                SELECT 
                                                                                                                                                                    ref.cd_raf_tete AS rule2_id_group_has_only_1_raf_with_level_3,
                                                                                                                                                                    watch.current_wl_level,
                                                                                                                                                                    COUNT(*) AS count_
                                                                                                                                                                FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                                                                                                LEFT JOIN
                                                                                                                                                                         (
                                                                                                                                                                            SELECT *
                                                                                                                                                                            FROM be4_ndod.tbl_global_watch
                                                                                                                                                                            WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                                                                                         ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                                                                                                WHERE ref.key_partition='202205310000' 
                                                                                                                                                                AND watch.current_wl_level = 3
                                                                                                                                                                GROUP BY ref.cd_raf_tete, watch.current_wl_level
                                                                                                                                                            ) rule2_id_group_has_only_1_raf_with_level_3
                                                                                                                                                        WHERE rule2_id_group_has_only_1_raf_with_level_3.count_ > 1
                                                                                                                                                      ) -- Im getting only the groups that has many rafs with wl = 3
                                                                                                                                --AND ref.cd_raf_tete = '0440312'
                                                                                                                                --AND ref.cd_raf_tete = '0491597'
                                                                                                                                --AND ref.cd_raf_tete = '0665914' -- PARA TIRAR 
                                                                                                                                --AND ref.cd_raf_tete = '0008062' -- PARA TIRAR
                                                                                                                                AND watch.current_wl_level = 3
                                                                                                                            )rule2_case2_manyrafs_with_level3
                                                                                                                        GROUP BY rule2_case2_manyrafs_with_level3.cd_raf_tete
                                                                                                                    )rule2_case2_manyrafs_with_level3_case_min_date
                                                                                                            )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes
                                                                                                        WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes.lesser_then_standard_date = 'yes' --GET THE ONES BEFORE 31/12/2020
                                                                                                     ) -- Here we do some calculations with groups that has many rafs and many of them has level 3. For those we calculate min date and check if is below or not then the standard one
                                            )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_almostrank1
                                    )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1
                                WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1.get_max_expo_rank = 1 --GET only the raf that has the max expo
                            ) rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1_final 
                        WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_yes_rank1_final.row_ = 1 --since there are ones that has the same expo/date_default status, then i have to retreive one random raf for those groups, in this case the one that appears in first place
                     )rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020 ON rule1.id_group=rule2_case2_manyrafs_with_level3_final_lesser_31_12_2020.cd_raf_tete 
--WHERE rule1.id_group LIKE '0008062'
--ORDER BY rule1.id_group DESC
            LEFT JOIN 
                     (
                        --TESTES AFTER 31/12/2020 [ONGOING]
                        SELECT 
                                DISTINCT
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1_final.cd_raf_tete,
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1_final.cd_raf,
                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1_final.default_status_date
                        FROM (
                                SELECT 
                                        rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1.*,
                                        ROW_NUMBER() OVER(PARTITION BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1.cd_raf_tete ORDER BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1.cd_raf DESC) AS row_
                                FROM (
                                        SELECT 
                                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.*,
                                                RANK() OVER(PARTITION BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.cd_raf_tete ORDER BY rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.mt_expo_global_ DESC, rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.default_status_date ASC) AS get_max_expo_rank
                                        FROM (
                                                SELECT 
                                                    rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join2.*,
                                                    digital.mt_expo_global,
                                                    CASE
                                                        WHEN digital.mt_expo_global IS NULL THEN 0
                                                        ELSE digital.mt_expo_global
                                                    END mt_expo_global_
                                                FROM (
                                                        SELECT 
                                                                rule2_case2_manyrafs_with_level3.*,
                                                                rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join.min_ -- This date is for the join to the digital
                                                                
                                                        FROM (
                                                                SELECT 
                                                                        ref.cd_raf_tete,
                                                                        ref.cd_raf,
                                                                        watch.current_wl_level,
                                                                        watch.default_status_date,
                                                                        ref.key_partition
                                                                        --ref.cd_raf_tete,
                                                                        --MIN(watch.default_status_date) AS date_
                                                                FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                    LEFT JOIN
                                                                             (
                                                                                SELECT *
                                                                                FROM be4_ndod.tbl_global_watch
                                                                                WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                             ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                WHERE ref.key_partition='202205310000' 
                                                                AND ref.cd_raf_tete IN 
                                                                                      (
                                                                                        SELECT *
                                                                                        FROM (
                                                                                                SELECT DISTINCT rule2_id_group_has_more_then_1_raf
                                                                                                FROM (
                                                                                                		SELECT 
                                                                                                		    cd_raf_tete AS rule2_id_group_has_more_then_1_raf,
                                                                                                		    count(*) AS count_
                                                                                                		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                                                                                                		WHERE key_partition='202205310000'
                                                                                                		GROUP BY cd_raf_tete
                                                                                                	) a
                                                                                                WHERE a.count_ > 1
                                                                                            ) rule2_id_group_has_more_then_1_raf 
                                                                                      ) -- Im getting only the groups that has more then 1 raf
                                                                AND ref.cd_raf_tete IN
                                                                                      (
                                                                                        SELECT DISTINCT rule2_id_group_has_only_1_raf_with_level_3
                                                                                        FROM (
                                                                                                SELECT 
                                                                                                    ref.cd_raf_tete AS rule2_id_group_has_only_1_raf_with_level_3,
                                                                                                    watch.current_wl_level,
                                                                                                    COUNT(*) AS count_
                                                                                                FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                                LEFT JOIN
                                                                                                         (
                                                                                                            SELECT *
                                                                                                            FROM be4_ndod.tbl_global_watch
                                                                                                            WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                         ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                                WHERE ref.key_partition='202205310000' 
                                                                                                AND watch.current_wl_level = 3
                                                                                                GROUP BY ref.cd_raf_tete, watch.current_wl_level
                                                                                            ) rule2_id_group_has_only_1_raf_with_level_3
                                                                                        WHERE rule2_id_group_has_only_1_raf_with_level_3.count_ > 1
                                                                                      ) -- Im getting only the groups that has many rafs with wl = 3
                                                                --AND ref.cd_raf_tete = '0440312'
                                                                --AND ref.cd_raf_tete = '0491597'
                                                                --AND ref.cd_raf_tete = '0665914' -- PARA TIRAR 
                                                                --AND ref.cd_raf_tete = '0008062' -- PARA TIRAR
                                                                AND watch.current_wl_level = 3
                                                                --GROUP BY ref.cd_raf_tete
                                                            ) rule2_case2_manyrafs_with_level3 
                                                                    INNER JOIN 
                                                                             (
                                                                             
                                                                                SELECT 
                                                                                    DISTINCT 
                                                                                    rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no.cd_raf_tete,
                                                                                    --rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no.cd_raf_tete, --TESTE
                                                                                    rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no.min_
                                                                                FROM (
                                                                                        SELECT 
                                                                                                rule2_case2_manyrafs_with_level3_case_min_date.*,
                                                                                                CASE
                                                                                                    WHEN rule2_case2_manyrafs_with_level3_case_min_date.min_ < '2020-12-31' THEN 'yes'
                                                                                                    ELSE 'no'
                                                                                                END lesser_then_standard_date
                                                                                        FROM (
                                                                                                SELECT 
                                                                                                        rule2_case2_manyrafs_with_level3.cd_raf_tete,
                                                                                                        MIN(rule2_case2_manyrafs_with_level3.default_status_date) AS min_
                                                                                                FROM (
                                                                                                        SELECT 
                                                                                                                ref.cd_raf_tete,
                                                                                                                ref.cd_raf,
                                                                                                                watch.current_wl_level,
                                                                                                                watch.default_status_date,
                                                                                                                ref.key_partition
                                                                                                                --ref.cd_raf_tete,
                                                                                                                --MIN(watch.default_status_date) AS date_
                                                                                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                                            LEFT JOIN
                                                                                                                     (
                                                                                                                        SELECT *
                                                                                                                        FROM be4_ndod.tbl_global_watch
                                                                                                                        WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                                     ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                                        WHERE ref.key_partition='202205310000' 
                                                                                                        AND ref.cd_raf_tete IN 
                                                                                                                              (
                                                                                                                                SELECT *
                                                                                                                                FROM (
                                                                                                                                        SELECT DISTINCT rule2_id_group_has_more_then_1_raf
                                                                                                                                        FROM (
                                                                                                                                        		SELECT 
                                                                                                                                        		    cd_raf_tete AS rule2_id_group_has_more_then_1_raf,
                                                                                                                                        		    count(*) AS count_
                                                                                                                                        		FROM gbn_credit_risk_reporting.tbl_cpty_ref
                                                                                                                                        		WHERE key_partition='202205310000'
                                                                                                                                        		GROUP BY cd_raf_tete
                                                                                                                                        	) a
                                                                                                                                        WHERE a.count_ > 1
                                                                                                                                    ) rule2_id_group_has_more_then_1_raf 
                                                                                                                              ) -- Im getting only the groups that has more then 1 raf
                                                                                                        AND ref.cd_raf_tete IN
                                                                                                                              (
                                                                                                                                SELECT DISTINCT rule2_id_group_has_only_1_raf_with_level_3
                                                                                                                                FROM (
                                                                                                                                        SELECT 
                                                                                                                                            ref.cd_raf_tete AS rule2_id_group_has_only_1_raf_with_level_3,
                                                                                                                                            watch.current_wl_level,
                                                                                                                                            COUNT(*) AS count_
                                                                                                                                        FROM gbn_credit_risk_reporting.tbl_cpty_ref ref
                                                                                                                                        LEFT JOIN
                                                                                                                                                 (
                                                                                                                                                    SELECT *
                                                                                                                                                    FROM be4_ndod.tbl_global_watch
                                                                                                                                                    WHERE key_partition='202205310000' -- HERE I WILL HAVE THE VALUE FROM PYTHON WITH THE LAST WORKING DAY OF THE PREVIOUS MONTH < CURRENT KP
                                                                                                                                                 ) watch ON ref.cd_raf=watch.cod_raf AND ref.key_partition=watch.key_partition
                                                                                                                                        WHERE ref.key_partition='202205310000' 
                                                                                                                                        AND watch.current_wl_level = 3
                                                                                                                                        GROUP BY ref.cd_raf_tete, watch.current_wl_level
                                                                                                                                    ) rule2_id_group_has_only_1_raf_with_level_3
                                                                                                                                WHERE rule2_id_group_has_only_1_raf_with_level_3.count_ > 1
                                                                                                                              ) -- Im getting only the groups that has many rafs with wl = 3
                                                                                                        --AND ref.cd_raf_tete = '0440312'
                                                                                                        --AND ref.cd_raf_tete = '0491597'
                                                                                                        --AND ref.cd_raf_tete = '0665914' -- PARA TIRAR 
                                                                                                        --AND ref.cd_raf_tete = '0008062' -- PARA TIRAR
                                                                                                        AND watch.current_wl_level = 3
                                                                                                    )rule2_case2_manyrafs_with_level3
                                                                                                GROUP BY rule2_case2_manyrafs_with_level3.cd_raf_tete
                                                                                            )rule2_case2_manyrafs_with_level3_case_min_date
                                                                                        --WHERE rule2_case2_manyrafs_with_level3_case_min_date.cd_raf_tete = '0002984'
                                                                                    )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no
                                                                                WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no.lesser_then_standard_date = 'no' --GET THE ONES AFTER 31/12/2020
                                                                             )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join ON rule2_case2_manyrafs_with_level3.cd_raf_tete=rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join.cd_raf_tete
                                                            --WHERE rule2_case2_manyrafs_with_level3.cd_raf_tete IN ('0002984')
                                                    )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join2
                                                            LEFT JOIN
                                                                      (
                                                                        SELECT 
                                                                            cd_raf, 
                                                                            CASE
                                                                                WHEN mt_expo_global IS NULL THEN 0
                                                                                ELSE mt_expo_global
                                                                            END mt_expo_global,
                                                                            key_partition
                                                                        FROM gbn_credit_risk_reporting.tbl_t_dtm_digital
                                                                        --WHERE key_partition = '202205310001' 
                                                                     ) digital ON rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join2.cd_raf=digital.cd_raf AND rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_join2.min_=cast(to_date(from_unixtime(unix_timestamp(substr(digital.key_partition,1,8), 'yyyyMMdd'))) as date)
                                                )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1
                                        --WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.cd_raf_tete IN ('0001734') -- para tirar
                                        --WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_almostrank1.mt_expo_global IS NOT NULL  -- para tirar
                                    )rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1
                                WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1.get_max_expo_rank = 1 --GET only the raf that has the max expo
                            ) rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1_final 
                        WHERE rule2_case2_manyrafs_with_level3_lesser_then_standard_date_no_rank1_final.row_ = 1 --since there are ones that has the same expo/date_default status, then i have to retreive one random raf for those groups, in this case the one that appears in first place
                     )rule2_case2_manyrafs_with_level3_final_after_31_12_2020 ON rule1.id_group=rule2_case2_manyrafs_with_level3_final_after_31_12_2020.cd_raf_tete 
