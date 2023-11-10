----------------------------------------- QUERY -------------------------------------------------------- (NOVO)
WITH BREF_FICHE as
(
                    Select cod_raf, 
                           cod_vision, 
                           num_fiche, 
                           dat_periode, 
                           dat_maj, 
                           flg_etat, 
                           key_partition 
                    From m24_trr.tbl_ta_bref_fiche fs 
                    where flg_etat = 'V' and cod_frequence >= 12 and cod_vision in ('C','S') and key_partition = '202205120000' 
                    and fs.dat_periode = (
                                            select max(dat_periode) 
                                            from m24_trr.tbl_ta_bref_fiche fm 
                                            where fm.cod_raf = fs.cod_raf and 
                                            fm.cod_frequence = fs.cod_frequence and 
                                            fm.flg_etat = fs.flg_etat 
                                            and fm.cod_vision = fs.cod_vision and key_partition = '202205120000' and flg_etat = 'V' 
                                            and cod_frequence >= 12 and cod_vision in ('C','S')
                    ) ORDER BY cod_raf DESC
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
    WHERE key_partition = (
                            SELECT MAX(key_partition) 
                            FROM m24_trr.tbl_ta_bref_fiche_detail_monthly
                        ) 
    AND cod_compte in ('Q93','l1','FL','E1','b9')
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
            --fiche_.dat_maj, 
            CASE
                WHEN fiche_.dat_maj IS NULL THEN cast(to_date(from_unixtime(unix_timestamp(substr('19000101',1,8), 'yyyyMMdd'))) as timestamp)
                ELSE fiche_.dat_maj
            END dat_maj_v2,
            detail_.mnt_compte, 
            CASE
                WHEN detail_.mnt_compte IS NULL THEN (SELECT MIN(mnt_compte) FROM BREF_FICHE_DETAIL)-1
                ELSE detail_.mnt_compte
            END mnt_compte_v2,
            detail_.dat_arrete, 
            CASE
                WHEN detail_.dat_arrete IS NULL THEN cast(to_date(from_unixtime(unix_timestamp(substr('19000101',1,8), 'yyyyMMdd'))) as date)
                ELSE detail_.dat_arrete
            END dat_arrete_v2,
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
    END has_ammount 
    FROM (
            SELECT 
                    cod_raf, 
                    SUM(mnt_compte) AS ammount 
            FROM BREF_FICHE_JOIN_DETAIL 
            GROUP BY cod_raf
    ) ammount
), 
BREF_FICHE_JOIN_DETAIL_FICHES AS 
(
    SELECT 
        cod_raf, COUNT(num_fiche) AS fiches_count 
        FROM BREF_FICHE_JOIN_DETAIL 
        GROUP BY cod_raf
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
BREF_FICHE_JOIN_DETAIL_MAIN AS 
(
    SELECT detail_.cod_raf, detail_.num_fiche, detail_.cod_vision, detail_.dat_arrete, detail_.key_partition,
    --detail_.*, 
    --ammount_.has_ammount, 
    --fiches_.fiches_count, 
    --check_.fiches_count_check,
    CASE 
        WHEN (ammount_.has_ammount = 1 AND dat_arrete > key_partition_modified_minus_18 AND fiches_.fiches_count <> check_.fiches_count_check) OR (ammount_.has_ammount = 1 AND dat_arrete > key_partition_modified_minus_18 AND fiches_.fiches_count = check_.fiches_count_check) THEN 'RULE2&3'
        ELSE RANK() OVER(PARTITION BY detail_.cod_raf ORDER BY detail_.dat_arrete_v2 DESC, detail_.dat_maj_v2 DESC, detail_.cod_vision_v2 DESC) -- Rule 1
    END rule1,
    CASE 
        WHEN (ammount_.has_ammount = 1) AND (dat_arrete > key_partition_modified_minus_18) AND (fiches_.fiches_count <> check_.fiches_count_check) THEN RANK() OVER(PARTITION BY detail_.cod_raf ORDER BY detail_.mnt_compte_v2 DESC) --Rule 2
        ELSE NULL
    END rule2,
    CASE 
        WHEN (ammount_.has_ammount = 1) AND (dat_arrete > key_partition_modified_minus_18) AND (fiches_.fiches_count = check_.fiches_count_check) THEN RANK() OVER(PARTITION BY detail_.cod_raf ORDER BY detail_.dat_arrete_v2 DESC, detail_.dat_maj_v2 DESC, detail_.cod_vision_v2 DESC)  -- Rule 3
        ELSE NULL
    END rule3
    FROM BREF_FICHE_JOIN_DETAIL detail_ 
        LEFT JOIN BREF_FICHE_JOIN_DETAIL_AMMOUNT ammount_ ON detail_.cod_raf = ammount_.cod_raf 
        LEFT JOIN BREF_FICHE_JOIN_DETAIL_FICHES fiches_ ON detail_.cod_raf = fiches_.cod_raf 
        LEFT JOIN BREF_FICHE_JOIN_DETAIL_FICHES_CHECK check_ ON detail_.cod_raf = check_.cod_raf
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
                        WHEN table_.rule1 = 1 OR table_.rule1 = 2 THEN 'O' 
                        WHEN table_.rule2 = 1 OR table_.rule3 = 1 THEN 'N' 
                        ELSE NULL 
                    END FLG_CA_RAF, 
                    table_.dat_arrete AS dat_last_ca, 
                    NULL AS dat_trigger_activation, 
                    table_.key_partition
            FROM (
                    SELECT cod_raf, num_fiche, cod_vision, rule1, rule2, rule3, dat_arrete, key_partition
                    FROM BREF_FICHE_JOIN_DETAIL_MAIN main
                    WHERE (main.rule1 = 1) OR (main.rule2 = 1) OR (main.rule3 = 1)
                    UNION
                    (
                        SELECT cod_raf, num_fiche, cod_vision, rule1, rule2, rule3, dat_arrete, key_partition
                        FROM BREF_FICHE_JOIN_DETAIL_MAIN main
                        WHERE cod_raf in (
                                            SELECT a.cod_raf
                                            FROM BREF_FICHE a
                                                 LEFT JOIN (SELECT DISTINCT cod_raf FROM BREF_FICHE_JOIN_DETAIL_MAIN main WHERE main.rule1 = 1 OR main.rule2 = 1 OR main.rule3 = 1) b ON a.cod_raf = b.cod_raf
                                            WHERE b.cod_raf IS NULL
                                        )
                        AND main.rule1 = 2
                    )
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
                                                            WHERE key_partition = (
                                                                                    SELECT MAX(key_partition) 
                                                                                    FROM m24_trr.tbl_bref_triggers
                                                                                    WHERE key_partition < '202205120000')
                                                            ) previous_key_partition ON current_key_partition.cod_raf = previous_key_partition.cod_raf
) 
SELECT *
FROM select_to_do