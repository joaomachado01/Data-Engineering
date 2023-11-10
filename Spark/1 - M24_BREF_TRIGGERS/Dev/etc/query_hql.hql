WITH BREF_FICHE as
(
    SELECT T1.cod_raf, T1.cod_vision, MAX(T1.num_fiche) as num_fiche, T2.dat_periode, T1.dat_maj, T1.flg_etat, T1.cod_frequence, T1.key_partition
FROM (
    SELECT fs.cod_raf, fs.cod_vision, fs.num_fiche, fs.dat_periode, fs.dat_maj, fs.flg_etat, fs.cod_frequence, fs.key_partition
    FROM m24_trr.tbl_ta_bref_fiche fs
    WHERE fs.key_partition = 'KP'
    AND fs.flg_etat = 'V'
    AND fs.cod_frequence >= 12
    AND fs.cod_vision IN ('C', 'S')
) T1
INNER JOIN (
    SELECT fm.cod_raf, fm.cod_frequence, fm.flg_etat, fm.cod_vision, max(dat_periode) AS dat_periode
    FROM m24_trr.tbl_ta_bref_fiche fm
    WHERE fm.key_partition = 'KP'
    AND fm.flg_etat = 'V'
    AND fm.cod_frequence >= 12
    AND fm.cod_vision IN ('C', 'S')
    GROUP BY fm.cod_raf, fm.cod_frequence, fm.flg_etat, fm.cod_vision 
) T2
ON T2.cod_raf = T1.cod_raf
AND T2.cod_frequence = T1.cod_frequence
AND T2.flg_etat = T1.flg_etat
AND T2.cod_vision = T1.cod_vision
AND T2.dat_periode = T1.dat_periode
GROUP BY 
T1.cod_raf, T1.cod_vision, T2.dat_periode, T1.dat_maj, T1.flg_etat, T1.cod_frequence, T1.key_partition
   ORDER BY cod_raf DESC
), 
BREF_FICHE_DETAIL AS 
(
    SELECT 
            num_fiche, 
            flg_etat, 
            dat_arrete, 
            mnt_compte,
            cod_compte
    FROM m24_trr.tbl_ta_bref_fiche_detail_monthly 
    WHERE key_partition = kp_max  -- VARIAVEL PYTHON 
    AND cod_compte in (turnovers)
), 
BREF_FICHE_JOIN_DETAIL AS 
(
    SELECT 
            fiche_.cod_raf, 
            fiche_.cod_vision,
            CASE
                WHEN fiche_.cod_vision IS NULL THEN 'A'
                ELSE fiche_.cod_vision
            END cod_vision_v2,
            fiche_.num_fiche, 
            CASE
                WHEN fiche_.dat_maj IS NULL THEN cast(to_date(from_unixtime(unix_timestamp(substr('19000101',1,8), 'yyyyMMdd'))) as timestamp)
                ELSE fiche_.dat_maj
            END dat_maj_v2,
            detail_.mnt_compte, 
            CASE
                WHEN detail_.mnt_compte IS NULL THEN (SELECT MIN(mnt_compte) FROM BREF_FICHE_DETAIL)-1
                ELSE detail_.mnt_compte
            END mnt_compte_v2,
            CASE
                WHEN fiche_.dat_periode IS NULL THEN cast(to_date(from_unixtime(unix_timestamp(substr('19000101',1,8), 'yyyyMMdd'))) as date)
                ELSE fiche_.dat_periode
            END dat_periode_v2,
            fiche_.dat_periode, 
            fiche_.key_partition, 
            add_months(cast(to_date(from_unixtime(unix_timestamp(substr(key_partition,1,8), 'yyyyMMdd'))) as date), -18) as key_partition_modified_minus_18 
            FROM BREF_FICHE fiche_ LEFT JOIN BREF_FICHE_DETAIL detail_ ON
                (fiche_.num_fiche=detail_.num_fiche AND 
                fiche_.flg_etat=detail_.flg_etat AND 
                fiche_.dat_periode=detail_.dat_arrete)
), 
BREF_FICHE_JOIN_DETAIL_AMMOUNT AS 
(
    SELECT ammount.cod_raf, 
    CASE 
        WHEN ammount IS NOT NULL THEN 1 
        ELSE NULL 
    END has_ammount,
    ammount.fiches_count
    FROM (
            SELECT 
                    cod_raf, 
                    SUM(mnt_compte) AS ammount,
                    COUNT(num_fiche) AS fiches_count
            FROM BREF_FICHE_JOIN_DETAIL 
            GROUP BY cod_raf
    ) ammount
), 
BREF_FICHE_JOIN_DETAIL_FICHES_CHECK AS 
(
    SELECT 
            a.cod_raf,
            CASE
                WHEN fiches_count_check IS NULL THEN -100
                ELSE fiches_count_check
            END fiches_count_check
    FROM (
            SELECT 
                    cod_raf, COUNT(num_fiche) AS fiches_count_check 
            FROM BREF_FICHE_JOIN_DETAIL 
            WHERE mnt_compte IS NOT NULL
            GROUP BY cod_raf
        ) a
),
BREF_FICHE_JOIN_DETAIL_MAIN_RULE2 AS 
(
    SELECT rule2tablefinal.cod_raf, rule2tablefinal.num_fiche, rule2tablefinal.cod_vision, rule2tablefinal.key_partition, rule2tablefinal.mnt_compte, rule2tablefinal.key_partition_modified_minus_18, rule2tablefinal.dat_periode, NULL AS rule1, rule2tablefinal.rank_rule2 AS rule2, NULL AS rule3
    FROM (
            SELECT rule2table.*, RANK() OVER(PARTITION BY rule2table.cod_raf ORDER BY rule2table.mnt_compte_v2 DESC) AS rank_rule2
            FROM (
                    SELECT detail_.cod_raf, detail_.num_fiche, detail_.cod_vision, detail_.key_partition, detail_.mnt_compte, detail_.mnt_compte_v2, detail_.key_partition_modified_minus_18, detail_.dat_periode,
                    CASE 
                        WHEN (ammount_.has_ammount = 1 AND detail_.mnt_compte IS NOT NULL) AND (detail_.dat_periode > detail_.key_partition_modified_minus_18) AND (ammount_.fiches_count <> check_.fiches_count_check) THEN "2"
                        ELSE NULL
                    END rule2
                    FROM BREF_FICHE_JOIN_DETAIL detail_ 
                        LEFT JOIN BREF_FICHE_JOIN_DETAIL_AMMOUNT ammount_ ON detail_.cod_raf = ammount_.cod_raf 
                        LEFT JOIN BREF_FICHE_JOIN_DETAIL_FICHES_CHECK check_ ON detail_.cod_raf = check_.cod_raf
                ) rule2table
            WHERE rule2table.rule2 IS NOT NULL
        ) rule2tablefinal
    WHERE rule2tablefinal.rank_rule2 = 1
),
BREF_FICHE_JOIN_DETAIL_MAIN_RULE3 AS 
(
    SELECT rule3tablefinal.cod_raf, rule3tablefinal.num_fiche, rule3tablefinal.cod_vision, rule3tablefinal.key_partition, rule3tablefinal.mnt_compte, rule3tablefinal.key_partition_modified_minus_18, rule3tablefinal.dat_periode, NULL AS rule1, NULL AS rule2, rule3tablefinal.rank_rule3 AS rule3
    FROM (
            SELECT rule3table.*, RANK() OVER(PARTITION BY rule3table.cod_raf ORDER BY rule3table.dat_periode_v2 DESC, rule3table.dat_maj_v2 DESC, rule3table.cod_vision_v2 DESC) AS rank_rule3
            FROM (
                    SELECT detail_.cod_raf, detail_.num_fiche, detail_.cod_vision, detail_.key_partition, detail_.mnt_compte, detail_.mnt_compte_v2, detail_.key_partition_modified_minus_18, detail_.dat_periode, detail_.cod_vision_v2, detail_.dat_periode_v2, detail_.dat_maj_v2,
                    CASE 
                        WHEN (ammount_.has_ammount = 1) AND (detail_.dat_periode > detail_.key_partition_modified_minus_18) AND (ammount_.fiches_count = check_.fiches_count_check) THEN "3"
                        ELSE NULL
                    END rule3
                    FROM BREF_FICHE_JOIN_DETAIL detail_ 
                        LEFT JOIN BREF_FICHE_JOIN_DETAIL_AMMOUNT ammount_ ON detail_.cod_raf = ammount_.cod_raf 
                        LEFT JOIN BREF_FICHE_JOIN_DETAIL_FICHES_CHECK check_ ON detail_.cod_raf = check_.cod_raf
                ) rule3table
            WHERE rule3table.rule3 IS NOT NULL
        ) rule3tablefinal
    WHERE rule3tablefinal.rank_rule3 = 1
),
BREF_FICHE_JOIN_DETAIL_MAIN_RULE1 AS 
(
    SELECT rule1tablefinal.cod_raf, rule1tablefinal.num_fiche, rule1tablefinal.cod_vision, rule1tablefinal.key_partition, rule1tablefinal.mnt_compte, rule1tablefinal.key_partition_modified_minus_18, rule1tablefinal.dat_periode, rule1tablefinal.rank_rule1 AS rule1, NULL AS rule2, NULL AS rule3
    FROM (
            SELECT detail_.cod_raf, detail_.num_fiche, detail_.cod_vision, detail_.key_partition, detail_.mnt_compte, detail_.key_partition_modified_minus_18, detail_.dat_periode, detail_.dat_periode_v2, detail_.dat_maj_v2, detail_.cod_vision_v2, RANK() OVER(PARTITION BY detail_.cod_raf ORDER BY detail_.dat_periode_v2 DESC, detail_.dat_maj_v2 DESC, detail_.cod_vision_v2 DESC) AS rank_rule1
            FROM BREF_FICHE_JOIN_DETAIL detail_ 
                LEFT JOIN BREF_FICHE_JOIN_DETAIL_AMMOUNT ammount_ ON detail_.cod_raf = ammount_.cod_raf 
                LEFT JOIN BREF_FICHE_JOIN_DETAIL_FICHES_CHECK check_ ON detail_.cod_raf = check_.cod_raf
            WHERE detail_.cod_raf NOT IN (SELECT cod_raf FROM BREF_FICHE_JOIN_DETAIL_MAIN_RULE2 UNION SELECT cod_raf FROM BREF_FICHE_JOIN_DETAIL_MAIN_RULE3)
        ) rule1tablefinal
    WHERE rule1tablefinal.rank_rule1 = 1
),
BREF_ALMOST_FINAL AS 
(
    SELECT 
        table_2.cod_raf, 
        table_2.num_fiche, 
        table_2.cod_vision, 
        table_2.FLG_CA_RAF, 
        table_2.dat_last_ca, 
        CASE 
            WHEN table_2.FLG_CA_RAF = 'O' THEN cast(to_date(from_unixtime(unix_timestamp(substr(table_2.key_partition,1,8), 'yyyyMMdd'))) as date) 
            ELSE NULL 
        END DAT_TRIGGER_ACTIVATION, 
        table_2.key_partition
    FROM (
            SELECT 
                    
                    table_.cod_raf, 
                    table_.num_fiche, 
                    table_.cod_vision, 
                    CASE 
                        WHEN table_.rule1 = 1 THEN 'O' 
                        WHEN table_.rule2 = 1 OR table_.rule3 = 1 THEN 'N' 
                        ELSE NULL 
                    END FLG_CA_RAF, 
                    table_.dat_periode AS dat_last_ca, 
                    NULL AS dat_trigger_activation, 
                    table_.key_partition
            FROM (
                    SELECT *
                    FROM BREF_FICHE_JOIN_DETAIL_MAIN_RULE1
                    UNION
                    SELECT *
                    FROM BREF_FICHE_JOIN_DETAIL_MAIN_RULE2
                    UNION 
                    SELECT *
                    FROM BREF_FICHE_JOIN_DETAIL_MAIN_RULE3
            ) table_
    )table_2
),
BREF_FINAL_TABLE AS 
(
    SELECT 
            current_key_partition.cod_raf, 
            current_key_partition.num_fiche, 
            current_key_partition.cod_vision, 
            current_key_partition.FLG_CA_RAF, 
            current_key_partition.DAT_LAST_CA,
            CASE 
                WHEN current_key_partition.FLG_CA_RAF = 'O' AND previous_key_partition.FLG_CA_RAF = 'O' THEN previous_key_partition.dat_trigger_activation 
                WHEN current_key_partition.FLG_CA_RAF = 'O' AND (previous_key_partition.FLG_CA_RAF = 'N' OR previous_key_partition.FLG_CA_RAF IS NULL) THEN cast(to_date(from_unixtime(unix_timestamp(substr(current_key_partition.key_partition,1,8), 'yyyyMMdd'))) as date) 
                ELSE NULL 
            END DAT_TRIGGER_ACTIVATION_CURRENT, 
            current_key_partition.key_partition 
    FROM BREF_ALMOST_FINAL current_key_partition LEFT JOIN (
                                                            SELECT cod_raf, FLG_CA_RAF, dat_trigger_activation 
                                                            FROM m24_trr.tbl_bref_triggers 
                                                            WHERE key_partition = max_kp_below_odate -- VARIAVEL PYTHON
                                                            ) previous_key_partition ON current_key_partition.cod_raf = previous_key_partition.cod_raf
) 
SELECT DISTINCT *
FROM select_to_do
