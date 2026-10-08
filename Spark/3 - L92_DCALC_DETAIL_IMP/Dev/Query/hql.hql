-- IMP


WITH select_to_do AS 
(
    SELECT 
            DISTINCT
            imp.idtierloctxt AS cod_raf,
            nvl(imp.nbjourimp, 0) AS mnt_nbj_retard_paiement,
            nvl(imp.nbjourimptiers, 0) AS mnt_nbj_impaye_tiers,
            imp.qualifimpaye AS cod_nature_impaye,
            watch.current_wl_level AS cod_niv_wl,
            imp.dtdebevt AS date_impaye_le_plus_ancien,
            imp.mtarriere AS mnt_impaye_le_plus_ancien,
            imp.partition_key,
            RANK() OVER(PARTITION BY imp.idtierloctxt ORDER BY nvl(imp.nbjourimp, 0) DESC, nvl(imp.nbjourimptiers, 0) DESC, imp.dtdebevt ASC, imp.mtarriere DESC) AS rank_
    FROM l97_fermat_conso.tbl_v_cust_ndod_imx_stock imp
            LEFT JOIN be4_ndod.tbl_global_watch watch ON (imp.idtierloctxt = watch.cod_raf AND imp.partition_key = watch.key_partition)
    WHERE imp.qualifimpaye NOT IN ('TECH', 'LITIGE')
    AND imp.partition_key = '[KP]'
)
SELECT 
        table_.cod_raf,
        table_.mnt_nbj_retard_paiement,
        table_.mnt_nbj_impaye_tiers,
        table_.cod_nature_impaye,
        table_.cod_niv_wl,
        CASE 
            WHEN table_.mnt_nbj_impaye_tiers > 90 THEN 'Y'
            ELSE 'N'
        END flg_is_defaut_paiement_sup_90j,
        CASE 
            WHEN table_.mnt_nbj_impaye_tiers > 30 THEN 'Y'
            ELSE 'N'
        END flg_is_defaut_paiement_sup_30j,
        CASE 
            WHEN table_.mnt_nbj_retard_paiement > 90 AND (table_.cod_niv_wl <> 3 OR table_.cod_niv_wl IS NULL) THEN 'Y'
            ELSE 'N'
        END flg_is_def_pmnt_90j_wl012,
        table_.date_impaye_le_plus_ancien,
        table_.mnt_impaye_le_plus_ancien,
        table_.partition_key
FROM  select_to_do table_
WHERE table_.rank_ = 1