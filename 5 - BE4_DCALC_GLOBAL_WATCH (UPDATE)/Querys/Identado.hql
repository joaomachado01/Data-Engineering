SELECT 
        current_key_partition.cod_tiers_r4,
        current_key_partition.cod_raf,
        current_key_partition.current_wl_level,
        current_key_partition.current_wl_level_date,
        current_key_partition.previous_wl_level,
        current_key_partition.previous_wl_level_date,
        current_key_partition.default_status,
        current_key_partition.default_status_date,
        current_key_partition.default_status_origin_cd,
        current_key_partition.forcing_comment,
        current_key_partition.flag_pp,
        current_key_partition.date_of_end_of_pp,
        current_key_partition.default_event_id,
        current_key_partition.default_code_event,
        current_key_partition.defaut_event_start_date,
        current_key_partition.flag_forbearance,
        current_key_partition.consolidated_total_arrears_amount,
        current_key_partition.consolidated_total_commitment,
        current_key_partition.imx_total_amount,
        current_key_partition.dax_total_amount,
        current_key_partition.commitment_date,
        current_key_partition.mdc_wl_discrepancy_flag,
        current_key_partition.significant_arrears_flag,
        current_key_partition.significant_arrears_type,
        current_key_partition.ntx_sub_past_due_flag,
        current_key_partition.other_sub_past_due_flag,
        current_key_partition.ntx_sub_forbearance_flag,
        current_key_partition.other_forbearance_flag,
        current_key_partition.non_default_30D_past_due_flag,
        current_key_partition.non_default_60D_past_due_flag,
        current_key_partition.phasys_type,
        CASE
            WHEN current_key_partition.phasys_type IS NULL THEN 'CIB_Business'
            ELSE current_key_partition.mgmt_entity
        END mgmt_entity--,
        --[phasys_type_date_update]
FROM (
        SELECT 
                -- base.*,
                ptr.phasys_type,
                ptr.mgmt_entity,
                CASE 
                    WHEN ptr.key_partition IS NULL THEN cast(to_date(from_unixtime(unix_timestamp(substr('202206010000',1,8), 'yyyyMMdd'))) as date)
                    ELSE cast(to_date(from_unixtime(unix_timestamp(substr(ptr.key_partition,1,8), 'yyyyMMdd'))) as date)
                END phasys_type_Date
        FROM
        (
            SELECT mwl.cod_tiers_r4                       AS cod_tiers_r4,
                m24raf.cod_codif_ext                   AS cod_raf,
                mwl.wl_level                           AS current_wl_level,
                mwl.date_start_wl                      AS current_wl_level_date,
                pwl.wl_level                           AS previous_wl_level,
                pwl.date_start_wl                      AS previous_wl_level_date,
                mwl.mdc_status                         AS default_status,
                mwl.mdc_status_update_date             AS default_status_date,
                mwl.mdc_status_origin                  AS default_status_origin_cd,
                mwl.mdc_forcing_comment                AS forcing_comment,
                CASE
                    WHEN mwl.mdc_top_pp = 1 THEN 'Y'
                    WHEN mwl.mdc_top_pp = 0 THEN 'N'
                    WHEN Lower(Cast(mwl.mdc_top_pp AS STRING)) = 'true' THEN 'Y'
                    WHEN Lower(Cast(mwl.mdc_top_pp AS STRING)) = 'false' THEN 'N'
                    ELSE NULL
                END                                    AS flag_pp,
                mwl.mdc_date_end_pp                    AS date_of_end_of_pp,
                mwl.event_id                           AS default_event_id,
                mwl.event_code                         AS default_code_event,
                mwl.mdc_default_entry_date             AS defaut_event_start_date,
                CASE
                    WHEN mdcs.top_forb = 1 THEN 'Y'
                    WHEN mdcs.top_forb = 0 THEN 'N'
                    WHEN Lower(Cast(mdcs.top_forb AS STRING)) = 'true' THEN 'Y'
                    WHEN Lower(Cast(mdcs.top_forb AS STRING)) = 'false' THEN 'N'
                    ELSE NULL
                END                                    AS flag_forbearance,
                mdcs.consolidated_total_arrears_amount AS
                consolidated_total_arrears_amount,
                mdcs.consolidated_total_commitment     AS consolidated_total_commitment,
                mdcstatus.entity_total_arrears_amount  AS imx_total_amount,
                mdcstatus.consolidated_total_arrears_amount -
                mdcstatus.entity_total_arrears_amount  AS dax_total_amount,
                mdcs.commitment_date                   AS commitment_date,
                CASE
                    WHEN mwl.wl_level = '3'
                        AND mwl.mdc_status = 'ND' THEN 'Y'
                    WHEN mwl.wl_level <> '3'
                        AND mwl.mdc_status = 'D' THEN 'Y'
                    ELSE 'N'
                END                                    AS mdc_wl_discrepancy_flag,
                CASE
                    WHEN Lower(Cast(mdcstatus.top_significant_arrears AS STRING)) = 'true'
                THEN
                    'Y'
                    ELSE 'N'
                END                                    AS significant_arrears_flag,
                mdcstatus.significant_arrears_step     AS significant_arrears_type,
                CASE
                    WHEN fich_flags.sm_ntx_sub_past_due_flag > 0 THEN 'Y'
                    ELSE 'N'
                END                                    AS ntx_sub_past_due_flag,
                CASE
                    WHEN fich_flags.sm_other_sub_past_due_flag > 0 THEN 'Y'
                    ELSE 'N'
                END                                    AS other_sub_past_due_flag,
                CASE
                    WHEN fich_flags.sm_ntx_sub_forbearance_flag > 0 THEN 'Y'
                    ELSE 'N'
                END                                    AS ntx_sub_forbearance_flag,
                CASE
                    WHEN fich_flags.sm_other_forbearance_flag > 0 THEN 'Y'
                    ELSE 'N'
                END                                    AS other_forbearance_flag,
                CASE
                    WHEN fch_thirty.cod_tiers_r4 IS NOT NULL THEN 'Y'
                    ELSE 'N'
                END                                    AS non_default_30D_past_due_flag,
                CASE
                    WHEN fch_sixty.cod_tiers_r4 IS NOT NULL THEN 'Y'
                    ELSE 'N'
                END                                    AS non_default_60D_past_due_flag
            FROM   be4_ndod.tbl_ta_msd_watchlist mwl
                INNER JOIN (SELECT wl.cod_tiers_r4,
                                    Max(wl.id) mx_id
                            FROM   be4_ndod.tbl_ta_msd_watchlist wl
                            WHERE  wl.key_partition = '202206010000'
                                    AND wl.date_end_wl IS NULL
                            GROUP  BY wl.cod_tiers_r4) wl
                        ON ( wl.cod_tiers_r4 = mwl.cod_tiers_r4
                                AND mwl.id = wl.mx_id )
                LEFT JOIN (SELECT mwl.id,
                                    mwl.wl_level,
                                    mwl.date_start_wl,
                                    mwl.cod_tiers_r4
                            FROM   be4_ndod.tbl_ta_msd_watchlist mwl
                                    INNER JOIN (SELECT wl.cod_tiers_r4,
                                                        Max(wl.id) AS ID
                                                FROM   be4_ndod.tbl_ta_msd_watchlist wl
                                                WHERE  wl.key_partition = '202206010000'
                                                        AND wl.date_end_wl IS NOT NULL
                                                GROUP  BY wl.cod_tiers_r4) prevl
                                            ON ( prevl.id = mwl.id
                                                AND
                                    prevl.cod_tiers_r4 = prevl.cod_tiers_r4 )
                            WHERE  mwl.key_partition = '202206010000') pwl
                        ON ( mwl.cod_tiers_r4 = pwl.cod_tiers_r4 )
                INNER JOIN (SELECT cod_codif_ext,
                                    cod_tiers_r4
                            FROM   m24_trr.tbl_ta_tiers_codif_ext
                            WHERE  cod_typ_codif = 'RAF'
                                    AND key_partition = '202206010000') m24raf
                        ON mwl.cod_tiers_r4 = m24raf.cod_tiers_r4
                LEFT JOIN (SELECT mdcst.cod_tiers_r4,
                                    mdcst.top_forb,
                                    mdcst.consolidated_total_arrears_amount,
                                    mdcst.consolidated_total_commitment,
                                    mdcst.commitment_date
                            FROM   (SELECT Max(id) AS mxid,
                                            cod_tiers_r4
                                    FROM   be4_ndod.tbl_ta_msd_mdc_status
                                    WHERE  key_partition = '202206010000'
                                    GROUP  BY cod_tiers_r4) mxmdcst
                                    INNER JOIN (SELECT id,
                                                        cod_tiers_r4,
                                                        top_forb,
                                                        consolidated_total_arrears_amount,
                                                        consolidated_total_commitment,
                                                        commitment_date
                                                FROM   be4_ndod.tbl_ta_msd_mdc_status
                                                WHERE  key_partition = '202206010000')
                                                mdcst
                                            ON mxmdcst.mxid = mdcst.id) mdcs
                        ON mdcs.cod_tiers_r4 = mwl.cod_tiers_r4
                LEFT JOIN (SELECT DISTINCT tmpW.cod_tiers_r4
                            FROM   (SELECT id
                                    FROM   be4_ndod.tbl_ta_msd_fiche
                                    WHERE  top_litigation = '0'
                                            AND top_technical = '0'
                                            AND cod_type_event IN ( 'IMX', 'DAX' )
                                            AND cod_statut = 'ACT'
                                            AND key_partition = '202206010000'
                                            AND date_event >= Date_sub(CURRENT_DATE, 30))
                                    fch
                                    INNER JOIN (SELECT event_id,
                                                        cod_tiers_r4
                                                FROM   be4_ndod.tbl_ta_msd_watchlist
                                                WHERE  key_partition = '202206010000'
                                                        AND mdc_status = 'ND')tmpW
                                            ON tmpW.event_id = fch.id) fch_thirty
                        ON fch_thirty.cod_tiers_r4 = mwl.cod_tiers_r4
                LEFT JOIN (SELECT DISTINCT tmpW.cod_tiers_r4
                            FROM   (SELECT id
                                    FROM   be4_ndod.tbl_ta_msd_fiche
                                    WHERE  top_litigation = '0'
                                            AND top_technical = '0'
                                            AND cod_type_event IN ( 'IMX', 'DAX' )
                                            AND cod_statut = 'ACT'
                                            AND key_partition = '202206010000'
                                            AND date_event >= Date_sub(CURRENT_DATE, 60))
                                    fch
                                    INNER JOIN (SELECT event_id,
                                                        cod_tiers_r4
                                                FROM   be4_ndod.tbl_ta_msd_watchlist
                                                WHERE  key_partition = '202206010000'
                                                        AND mdc_status = 'ND')tmpW
                                            ON tmpW.event_id = fch.id) fch_sixty
                        ON fch_thirty.cod_tiers_r4 = mwl.cod_tiers_r4
                LEFT JOIN (SELECT id,
                                    event_type_cod,
                                    cod_raf
                            FROM   be4_ndod.tbl_global_fiche
                            WHERE  key_partition = '202206010000'
                                    AND status_cod = 'ACT'
                                    AND event_type_cod LIKE 'AR%') sigarrtyp
                        ON ( sigarrtyp.id = mwl.event_id
                            AND sigarrtyp.cod_raf = m24raf.cod_codif_ext )
                LEFT JOIN (SELECT cod_raf,
                                    Sum (mltflgs.significant_arrears_flag) AS
                        sm_significant_arrears_flag,
                                    Sum (mltflgs.ntx_sub_past_due_flag)    AS
                        sm_ntx_sub_past_due_flag,
                                    Sum (mltflgs.other_sub_past_due_flag)  AS
                        sm_other_sub_past_due_flag,
                                    Sum (mltflgs.ntx_sub_forbearance_flag) AS
                        sm_ntx_sub_forbearance_flag,
                                    Sum (mltflgs.other_forbearance_flag)   AS
                        sm_other_forbearance_flag
                            FROM   (SELECT cod_raf,
                                            CASE
                                            WHEN status_cod = 'ACT'
                                                    AND event_type_cod LIKE 'AR%' THEN 1
                                            ELSE 0
                                            END AS significant_arrears_flag,
                                            CASE
                                            WHEN event_type_cod IN ( 'IMX', 'DAX' )
                                                    AND status_cod = 'ACT'
                                                    AND source_bank IN ( 30007, 11470, 18919
                                                                    ) THEN
                                            1
                                            ELSE 0
                                            END AS ntx_sub_past_due_flag,
                                            CASE
                                            WHEN event_type_cod IN ( 'IMX', 'DAX' )
                                                    AND status_cod = 'ACT'
                                                    AND source_bank NOT IN (
                                                        30007, 11470, 18919 )
                                            THEN 1
                                            ELSE 0
                                            END AS other_sub_past_due_flag,
                                            CASE
                                            WHEN event_type_cod = 'F'
                                                    AND status_cod = 'ACT'
                                                    AND source_bank IN ( 30007, 11470, 18919
                                                                    ) THEN
                                            1
                                            ELSE 0
                                            END AS ntx_sub_forbearance_flag,
                                            CASE
                                            WHEN event_type_cod = 'F'
                                                    AND status_cod = 'ACT'
                                                    AND source_bank NOT IN (
                                                        30007, 11470, 18919 )
                                            THEN 1
                                            ELSE 0
                                            END AS other_forbearance_flag
                                    FROM   be4_ndod.tbl_global_fiche
                                    WHERE  key_partition = '202206010000') mltflgs
                            GROUP  BY cod_raf) fich_flags
                        ON fich_flags.cod_raf = m24raf.cod_codif_ext
                LEFT JOIN (SELECT *
                            FROM   be4_ndod.tbl_ta_msd_mdc_status
                            WHERE  key_partition = '202206010000') mdcstatus
                        ON ( mdcstatus.cod_tiers_r4 = mwl.cod_tiers_r4
                            AND mdcstatus.bank_code = mwl.mdc_bank_code )
            WHERE  mwl.key_partition = '202206010000'
            UNION
            SELECT DISTINCT b.cod_tiers_r4                        AS cod_tiers_r4,
                            b.cod_raf                             AS cod_raf,
                            b.current_wl_level                    AS current_wl_level,
                            b.current_wl_level_date               AS current_wl_level_date,
                            b.previous_wl_level                   AS previous_wl_level,
                            b.previous_wl_level_date              AS previous_wl_level_date,
                            mdcstatus.status                      AS default_status,
                            mdcstatus.status_update_date          AS default_status_date,
                            mdcstatus.origin                      AS
                            default_status_origin_cd,
                            mdcstatus.forcing_comment             AS forcing_comment,
                            CASE
                            WHEN mdcstatus.top_pp = 1 THEN 'Y'
                            WHEN mdcstatus.top_pp = 0 THEN 'N'
                            WHEN Lower(Cast(mdcstatus.top_pp AS STRING)) = 'true' THEN 'Y'
                            WHEN Lower(Cast(mdcstatus.top_pp AS STRING)) = 'false' THEN
                            'N'
                            ELSE NULL
                            END                                   AS flag_pp,
                            mdcstatus.date_end_pp                 AS date_of_end_of_pp,
                            b.default_event_id                    AS default_event_id,
                            b.default_code_event                  AS default_code_event,
                            mdcstatus.default_entry_date          AS defaut_event_start_date
                            ,
                            b.flag_forbearance                    AS
                            flag_forbearance,
                            b.consolidated_total_arrears_amount   AS
                            consolidated_total_arrears_amount,
                            b.consolidated_total_commitment       AS
                            consolidated_total_commitment,
                            mdcstatus.entity_total_arrears_amount AS imx_total_amount,
                            mdcstatus.consolidated_total_arrears_amount -
                            mdcstatus.entity_total_arrears_amount AS dax_total_amount,
                            b.commitment_date                     AS commitment_date,
                            CASE
                            WHEN b.current_wl_level = '3'
                                AND mdcstatus.status = 'ND' THEN 'Y'
                            WHEN b.current_wl_level <> '3'
                                AND mdcstatus.status = 'D' THEN 'Y'
                            ELSE 'N'
                            END                                   AS mdc_wl_discrepancy_flag
                            ,
                            CASE
                            WHEN Lower(Cast(mdcstatus.top_significant_arrears AS STRING))
                                = 'true' THEN
                            'Y'
                            ELSE 'N'
                            END                                   AS
                            significant_arrears_flag,
                            mdcstatus.significant_arrears_step    AS
                            significant_arrears_type,
                            CASE
                            WHEN fich_flags.sm_ntx_sub_past_due_flag > 0 THEN 'Y'
                            ELSE 'N'
                            END                                   AS ntx_sub_past_due_flag,
                            CASE
                            WHEN fich_flags.sm_other_sub_past_due_flag > 0 THEN 'Y'
                            ELSE 'N'
                            END                                   AS other_sub_past_due_flag
                            ,
                            CASE
                            WHEN fich_flags.sm_ntx_sub_forbearance_flag > 0 THEN 'Y'
                            ELSE 'N'
                            END                                   AS
                            ntx_sub_forbearance_flag,
                            CASE
                            WHEN fich_flags.sm_other_forbearance_flag > 0 THEN 'Y'
                            ELSE 'N'
                            END                                   AS other_forbearance_flag,
                            CASE
                            WHEN fch_thirty.cod_tiers_r4 IS NOT NULL THEN 'Y'
                            ELSE 'N'
                            END                                   AS
                            non_default_30D_past_due_flag,
                            CASE
                            WHEN fch_sixty.cod_tiers_r4 IS NOT NULL THEN 'Y'
                            ELSE 'N'
                            END                                   AS
                            non_default_60D_past_due_flag
            FROM   (SELECT mdcs.cod_tiers_r4                      AS cod_tiers_r4,
                        m24raf.cod_codif_ext                   AS cod_raf,
                        a.current_wl_level                     AS current_wl_level,
                        a.current_wl_level_date                AS current_wl_level_date,
                        a.previous_wl_level                    AS previous_wl_level,
                        a.previous_wl_level_date               AS previous_wl_level_date,
                        a.default_event_id                     AS default_event_id,
                        a.default_code_event                   AS default_code_event,
                        CASE
                            WHEN mdcs.top_forb = 1 THEN 'Y'
                            WHEN mdcs.top_forb = 0 THEN 'N'
                            WHEN Lower(Cast(mdcs.top_forb AS STRING)) = 'true' THEN 'Y'
                            WHEN Lower(Cast(mdcs.top_forb AS STRING)) = 'false' THEN 'N'
                            ELSE NULL
                        END                                    AS flag_forbearance,
                        mdcs.consolidated_total_arrears_amount AS
                                consolidated_total_arrears_amount,
                        mdcs.consolidated_total_commitment     AS
                        consolidated_total_commitment,
                        mdcs.commitment_date                   AS commitment_date
                    FROM   (SELECT mwl.cod_tiers_r4  AS cod_tiers_r4,
                                mwl.wl_level      AS current_wl_level,
                                mwl.date_start_wl AS current_wl_level_date,
                                pwl.wl_level      AS previous_wl_level,
                                pwl.date_start_wl AS previous_wl_level_date,
                                mwl.event_id      AS default_event_id,
                                mwl.event_code    AS default_code_event
                            FROM   be4_ndod.tbl_ta_msd_watchlist mwl
                                INNER JOIN (SELECT wl.cod_tiers_r4,
                                                    Max(wl.id) mx_id
                                            FROM   be4_ndod.tbl_ta_msd_watchlist wl
                                            WHERE  wl.key_partition = '202206010000'
                                                    AND wl.date_end_wl IS NULL
                                            GROUP  BY wl.cod_tiers_r4) wl
                                        ON ( wl.cod_tiers_r4 = mwl.cod_tiers_r4
                                                AND mwl.id = wl.mx_id )
                                LEFT JOIN (SELECT mwl.id,
                                                    mwl.wl_level,
                                                    mwl.date_start_wl,
                                                    mwl.cod_tiers_r4
                                            FROM   be4_ndod.tbl_ta_msd_watchlist mwl
                                                    INNER JOIN
                                                    (SELECT wl.cod_tiers_r4,
                                                            Max(wl.id) AS ID
                                                    FROM   be4_ndod.tbl_ta_msd_watchlist
                                                            wl
                                                    WHERE  wl.key_partition =
                                                            '202206010000'
                                                            AND wl.date_end_wl IS NOT NULL
                                                    GROUP  BY wl.cod_tiers_r4) prevl
                                                            ON ( prevl.id = mwl.id
                                                                AND
            prevl.cod_tiers_r4 = prevl.cod_tiers_r4 )
            WHERE  mwl.key_partition = '202206010000') pwl
            ON ( mwl.cod_tiers_r4 = pwl.cod_tiers_r4 )
            WHERE  mwl.key_partition = '202206010000') a
            RIGHT JOIN (SELECT mdcst.cod_tiers_r4,
            mdcst.top_forb,
            mdcst.consolidated_total_arrears_amount,
            mdcst.consolidated_total_commitment,
            mdcst.commitment_date
            FROM   (SELECT Max(id) AS mxid,
            cod_tiers_r4
            FROM   be4_ndod.tbl_ta_msd_mdc_status
            WHERE  key_partition = '202206010000'
            GROUP  BY cod_tiers_r4) mxmdcst
            INNER JOIN (SELECT id,
                        cod_tiers_r4,
                        top_forb,
                        consolidated_total_arrears_amount,
                        consolidated_total_commitment,
                        commitment_date
                FROM   be4_ndod.tbl_ta_msd_mdc_status
                WHERE  key_partition = '202206010000')
                mdcst
            ON mxmdcst.mxid = mdcst.id) mdcs
            ON mdcs.cod_tiers_r4 = a.cod_tiers_r4
            INNER JOIN (SELECT cod_codif_ext,
            cod_tiers_r4
            FROM   m24_trr.tbl_ta_tiers_codif_ext
            WHERE  cod_typ_codif = 'RAF'
            AND key_partition = '202206010000') m24raf
            ON mdcs.cod_tiers_r4 = m24raf.cod_tiers_r4
            WHERE  a.cod_tiers_r4 IS NULL) b
            LEFT JOIN (SELECT DISTINCT tmpW.cod_tiers_r4
            FROM   (SELECT id
            FROM   be4_ndod.tbl_ta_msd_fiche
            WHERE  top_litigation = '0'
            AND top_technical = '0'
            AND cod_type_event IN ( 'IMX', 'DAX' )
            AND cod_statut = 'ACT'
            AND key_partition = '202206010000'
            AND date_event >= Date_sub(CURRENT_DATE, 30)) fch
            INNER JOIN (SELECT event_id,
            cod_tiers_r4
            FROM   be4_ndod.tbl_ta_msd_watchlist
            WHERE  key_partition = '202206010000'
            AND mdc_status = 'ND')tmpW
            ON tmpW.event_id = fch.id) fch_thirty
            ON fch_thirty.cod_tiers_r4 = b.cod_tiers_r4
            LEFT JOIN (SELECT DISTINCT tmpW.cod_tiers_r4
            FROM   (SELECT id
            FROM   be4_ndod.tbl_ta_msd_fiche
            WHERE  top_litigation = '0'
            AND top_technical = '0'
            AND cod_type_event IN ( 'IMX', 'DAX' )
            AND cod_statut = 'ACT'
            AND key_partition = '202206010000'
            AND date_event >= Date_sub(CURRENT_DATE, 60)) fch
            INNER JOIN (SELECT event_id,
            cod_tiers_r4
            FROM   be4_ndod.tbl_ta_msd_watchlist
            WHERE  key_partition = '202206010000'
            AND mdc_status = 'ND') tmpW
            ON tmpW.event_id = fch.id) fch_sixty
            ON fch_thirty.cod_tiers_r4 = b.cod_tiers_r4
            LEFT JOIN (SELECT id,
            event_type_cod,
            cod_raf
            FROM   be4_ndod.tbl_global_fiche
            WHERE  key_partition = '202206010000'
            AND status_cod = 'ACT'
            AND event_type_cod LIKE 'AR%') sigarrtyp
            ON ( sigarrtyp.id = b.default_event_id
            AND sigarrtyp.cod_raf = b.cod_raf )
            LEFT JOIN (SELECT cod_raf,
            Sum (mltflgs.significant_arrears_flag) AS
            sm_significant_arrears_flag,
            Sum (mltflgs.ntx_sub_past_due_flag)    AS
            sm_ntx_sub_past_due_flag,
            Sum (mltflgs.other_sub_past_due_flag)  AS
            sm_other_sub_past_due_flag,
            Sum (mltflgs.ntx_sub_forbearance_flag) AS
            sm_ntx_sub_forbearance_flag,
            Sum (mltflgs.other_forbearance_flag)   AS
            sm_other_forbearance_flag
            FROM   (SELECT cod_raf,
            CASE
            WHEN status_cod = 'ACT'
            AND event_type_cod LIKE 'AR%' THEN 1
            ELSE 0
            END AS significant_arrears_flag,
            CASE
            WHEN event_type_cod IN ( 'IMX', 'DAX' )
            AND status_cod = 'ACT'
            AND source_bank IN ( 30007, 11470, 18919 ) THEN
            1
            ELSE 0
            END AS ntx_sub_past_due_flag,
            CASE
            WHEN event_type_cod IN ( 'IMX', 'DAX' )
            AND status_cod = 'ACT'
            AND source_bank NOT IN ( 30007, 11470, 18919 )
            THEN 1
            ELSE 0
            END AS other_sub_past_due_flag,
            CASE
            WHEN event_type_cod = 'F'
            AND status_cod = 'ACT'
            AND source_bank IN ( 30007, 11470, 18919 ) THEN
            1
            ELSE 0
            END AS ntx_sub_forbearance_flag,
            CASE
            WHEN event_type_cod = 'F'
            AND status_cod = 'ACT'
            AND source_bank NOT IN ( 30007, 11470, 18919 )
            THEN 1
            ELSE 0
            END AS other_forbearance_flag
            FROM   be4_ndod.tbl_global_fiche
            WHERE  key_partition = '202206010000') mltflgs
            GROUP  BY cod_raf) fich_flags
            ON fich_flags.cod_raf = b.cod_raf
            LEFT JOIN (SELECT status_n.cod_tiers_r4,
            status_n.status_update_date,
            status_n.last_update_date,
            bank_code,
            status_n.status,
            status_n.origin,
            status_n.forcing_comment,
            status_n.top_pp,
            date_end_pp,
            status_n.default_entry_date,
            status_n.entity_total_arrears_amount,
            status_n.consolidated_total_arrears_amount,
            status_n.top_significant_arrears,
            status_n.significant_arrears_step
            FROM   be4_ndod.tbl_ta_msd_mdc_status status_n
            INNER JOIN (SELECT mx_status.cod_tiers_r4,
            mx_status.status_update_date,
            mx_status_2.last_update_date,
            Max(mx_status_2.bank_code) AS mx_bank_code
            FROM   (SELECT mx_status.cod_tiers_r4,
                    mx_status_2.status_update_date,
                    Max(mx_status_2.last_update_date)
                    AS
                    mx_last_update_date
                FROM
            (SELECT cod_tiers_r4,
            Max(status_update_date) AS
            mx_status_update_date
            FROM   be4_ndod.tbl_ta_msd_mdc_status
            WHERE  key_partition = '202206010000'
            GROUP  BY cod_tiers_r4) mx_status
            INNER JOIN
            (SELECT cod_tiers_r4,
            status_update_date,
            last_update_date
            FROM   be4_ndod.tbl_ta_msd_mdc_status
            WHERE  key_partition = '202206010000')
            mx_status_2
            ON ( mx_status.cod_tiers_r4 =
                    mx_status_2.cod_tiers_r4
                    AND
            mx_status.mx_status_update_date =
            mx_status_2.status_update_date )
                GROUP  BY mx_status.cod_tiers_r4,
                        mx_status_2.status_update_date)
            mx_status
            INNER JOIN
            (SELECT cod_tiers_r4,
                    status_update_date,
                    last_update_date,
                    bank_code
                FROM   be4_ndod.tbl_ta_msd_mdc_status
                WHERE  key_partition = '202206010000')
            mx_status_2
                    ON ( mx_status.cod_tiers_r4 =
                            mx_status_2.cod_tiers_r4
                            AND
            mx_status.status_update_date =
            mx_status_2.status_update_date
                            AND
            mx_status.mx_last_update_date =
            mx_status_2.last_update_date )
            GROUP  BY mx_status.cod_tiers_r4,
                mx_status.status_update_date,
                mx_status_2.last_update_date) mx_status
            ON ( status_n.cod_tiers_r4 = mx_status.cod_tiers_r4
            AND status_n.status_update_date =
            mx_status.status_update_date
            AND status_n.last_update_date =
            mx_status.last_update_date
            AND status_n.bank_code = mx_status.mx_bank_code )
            WHERE  status_n.key_partition = '202206010000') mdcstatus
            ON ( mdcstatus.cod_tiers_r4 = b.cod_tiers_r4 ) 
        ) base
                        LEFT JOIN
                            (
                            SELECT 
                                    rule1.cod_tiers_r4,
                                    rule1.cod_raf,
                                    rule1.key_partition,
                                    rule1.phasys_type,
                                    CASE
                                        WHEN rule1.phasys_type = 'AMI' THEN
                                            CASE
                                                WHEN rule2_case1_caso1 IS NOT NULL OR (rule2_case2_caso1.event_date > rule2_case2_caso2.event_date) THEN 'DRAS_ Restructuring'
                                                ELSE 'CIB_Business'
                                            END
                                        WHEN rule1.phasys_type = 'LIQ' THEN
                                            CASE
                                                WHEN rule2_case3_caso1.rule2_case3_caso1 IS NOT NULL THEN 'DRAS'
                                                WHEN rule2_case4_caso1.rule2_case4_caso1 IS NOT NULL THEN 'LRG'
                                                ELSE 'CIB_Business'
                                            END
                                        WHEN rule1.phasys_type IS NULL THEN 'CIB_Business'
                                        ELSE NULL
                                    END mgmt_entity
                            FROM (
                                    SELECT 
                                        DISTINCT
                                        rule1main.cod_raf,
                                        rule1main.cod_tiers_r4, 
                                        rule1main.phasys_type,
                                        rule1main.key_partition
                                    FROM (
                                            SELECT 
                                                base.cod_tiers_r4,
                                                base.cod_raf,
                                                base.event_date,
                                                base.id,
                                                base.event_type_cod,
                                                base.key_partition,
                                                CASE
                                                    WHEN case1.case1 IS NOT NULL THEN 'LIQ'
                                                    WHEN case2_a.event_date < case2_b.event_date THEN 'LIQ'
                                                    WHEN case3_a.event_date > case3_a.event_date THEN 'AMI'
                                                    WHEN case4_a IS NOT NULL THEN 'AMI'
                                                    WHEN case4_b IS NOT NULL THEN NULL
                                                    ELSE NULL
                                                END phasys_type
                                            FROM (
                                                    SELECT 
                                                        *
                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                ) base LEFT JOIN 
                                                                (
                                                                    SELECT 
                                                                        DISTINCT a.attribute_value_2, a.net_mnt, a.case1
                                                                    FROM (
                                                                            SELECT 
                                                                                c.attribute_value_2,
                                                                                c.net_mnt,
                                                                                CASE
                                                                                    WHEN c.net_mnt IS NOT NULL THEN
                                                                                        CASE
                                                                                            WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                CASE
                                                                                                    WHEN sigarrtyp.event_type_cod = 'LJ' AND sigarrtyp.status_cod = 'ACT' THEN 'case1'
                                                                                                    ELSE NULL
                                                                                                END
                                                                                            ELSE NULL 
                                                                                        END
                                                                                    ELSE NULL
                                                                                END case1
                                                                            FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                            WHERE sigarrtyp.key_partition = '202206010000'
                                                                        ) a 
                                                                    WHERE a.case1 IS NOT NULL
                                                                
                                                                )case1 ON base.cod_raf = case1.attribute_value_2
                                                    LEFT JOIN 
                                                                ( 
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod = 'DM01' AND sigarrtyp.status_cod = 'ACT' THEN 'case2_a'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case2_a
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case2_a IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                        
                                                                )case2_a ON base.cod_raf = case2_a.attribute_value_2
                                                    LEFT JOIN 
                                                                (
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_ 
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod IN ('DT', 'DN') AND sigarrtyp.status_cod = 'ACT' THEN 'case2_b'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case2_b
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case2_b IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                                )case2_b ON base.cod_raf = case2_b.attribute_value_2
                                                    LEFT JOIN 
                                                                (
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod = 'DM01' AND sigarrtyp.status_cod = 'ACT' THEN 'case3_a'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case3_a
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case3_a IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                                
                                                                )case3_a ON base.cod_raf = case3_a.attribute_value_2
                                            
                                                    LEFT JOIN 
                                                                (
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod IN ('DT', 'DN') AND sigarrtyp.status_cod = 'ACT' THEN 'case3_b'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case3_b
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case3_b IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                                
                                                                )case3_b ON base.cod_raf = case3_b.attribute_value_2
                                                    LEFT JOIN
                                                                (
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod IN ('DT', 'DN', 'AM', 'AR3', 'CAD', 'CB', 'CC', 'CDX', 'CE', 'CMA', 'CMD', 'CN', 'CPS', 'D1', 'DA', 'DC', 'DE', 'DH', 'DI', 'DM00', 'DM01', 'DM02', 'DM03', 'DM04', 'DM05', 'DM06', 'DM07', 'DM08', 'DP', 'DR', 'DUR', 'G1', 'GR', 'IB', 'IF', 'IJU', 'IM', 'IO', 'IP', 'IR', 'LS', 'MA', 'NA', 'NR', 'NT', 'PC', 'PC1', 'PCO', 'PE', 'PH', 'PP', 'PRP', 'PSU', 'RAD', 'RG', 'RJ', 'RP', 'RT', 'S2', 'SA', 'SD', 'SG', 'SI') AND sigarrtyp.status_cod = 'ACT' THEN 'case4_a'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case4_a
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case4_a IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                                )case4_a ON base.cod_raf = case4_a.attribute_value_2
                                                    LEFT JOIN
                                                                (
                                                                    SELECT 
                                                                        *
                                                                    FROM (
                                                                            SELECT 
                                                                                *,
                                                                                RANK() OVER(PARTITION BY a.attribute_value_2 ORDER BY a.event_date DESC, a.id DESC) AS rank_ 
                                                                            FROM (
                                                                                    SELECT 
                                                                                        c.attribute_value_2,
                                                                                        sigarrtyp.id,
                                                                                        sigarrtyp.event_date,
                                                                                        CASE
                                                                                            WHEN c.net_mnt IS NOT NULL THEN
                                                                                                CASE
                                                                                                    WHEN c.attribute_value_1 = '000' AND  c.attribute_value_3 = 'GLOBAL' AND c.flexible_key_name = 'CptyReport' AND c.attribute_value_4 = '0D' THEN 
                                                                                                        CASE
                                                                                                            WHEN sigarrtyp.event_type_cod IN ('AR0', 'AR1', 'AR2', 'DAX', 'F', 'IMX', 'SS', 'W1', 'W2') AND sigarrtyp.status_cod = 'ACT' THEN 'case4_b'
                                                                                                            ELSE NULL
                                                                                                        END
                                                                                                    ELSE NULL 
                                                                                                END
                                                                                            ELSE NULL
                                                                                        END case4_b
                                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                                        LEFT JOIN (SELECT * FROM l97_fermat_conso.tbl_cust_detail_exposure WHERE partition_key = '202205310000') c ON (c.attribute_value_2 = sigarrtyp.cod_raf)
                                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                                ) a 
                                                                            WHERE a.case4_b IS NOT NULL
                                                                        ) b 
                                                                    WHERE b.rank_ = 1
                                                                )case4_b ON base.cod_raf = case4_b.attribute_value_2
                                         )rule1main                  
                                    ) rule1 LEFT JOIN 
                                                    (
                                                    SELECT 
                                                            *
                                                    FROM (
                                                            SELECT 
                                                                    *,
                                                                    RANK() OVER(PARTITION BY a.cod_raf ORDER BY a.id ASC) AS rank_
                                                            FROM (
                                                                    SELECT 
                                                                            sigarrtyp.cod_raf,
                                                                            sigarrtyp.event_date,
                                                                            sigarrtyp.id,
                                                                            cpty.cd_prtf_rsk,
                                                                            sigarrtyp.event_type_cod,
                                                                            CASE
                                                                                WHEN sigarrtyp.event_type_cod IN ('SA', 'S2', 'SG', 'RJ', 'PC1') THEN 'rule2_case1_caso1'
                                                                                ELSE NULL 
                                                                            END rule2_case1_caso1
                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                        LEFT JOIN gbn_credit_risk_reporting.tbl_cpty_ref cpty ON (cpty.cd_raf = sigarrtyp.cod_raf AND cpty.key_partition = sigarrtyp.key_partition)
                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                ) a 
                                                            WHERE a.rule2_case1_caso1 IS NOT NULL
                                                        ) b 
                                                    WHERE b.rank_ = 1
                                                    )rule2_case1_caso1 ON rule1.cod_raf = rule2_case1_caso1.cod_raf
                                            LEFT JOIN
                                                    (
                                                    SELECT 
                                                            *
                                                    FROM (
                                                            SELECT 
                                                                    *,
                                                                    RANK() OVER(PARTITION BY a.cod_raf ORDER BY a.id ASC) AS rank_
                                                            FROM (
                                                                    SELECT 
                                                                            sigarrtyp.cod_raf,
                                                                            sigarrtyp.event_date,
                                                                            sigarrtyp.id,
                                                                            cpty.cd_prtf_rsk,
                                                                            sigarrtyp.event_type_cod,
                                                                            CASE
                                                                                WHEN sigarrtyp.event_type_cod = 'DM01' AND sigarrtyp.status_cod = 'ACT' THEN 'rule2_case2_caso1'
                                                                                ELSE NULL 
                                                                            END rule2_case2_caso1
                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                        LEFT JOIN gbn_credit_risk_reporting.tbl_cpty_ref cpty ON (cpty.cd_raf = sigarrtyp.cod_raf AND cpty.key_partition = sigarrtyp.key_partition)
                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                ) a 
                                                            WHERE a.rule2_case2_caso1 IS NOT NULL
                                                        ) b 
                                                    WHERE b.rank_ = 1
                                                    ) rule2_case2_caso1 ON rule1.cod_raf = rule2_case2_caso1.cod_raf
                                            LEFT JOIN
                                                    (
                                                    SELECT 
                                                            *
                                                    FROM (
                                                            SELECT 
                                                                    *,
                                                                    RANK() OVER(PARTITION BY a.cod_raf ORDER BY a.id ASC) AS rank_
                                                            FROM (
                                                                    SELECT 
                                                                            sigarrtyp.cod_raf,
                                                                            sigarrtyp.event_date,
                                                                            sigarrtyp.id,
                                                                            cpty.cd_prtf_rsk,
                                                                            sigarrtyp.event_type_cod,
                                                                            CASE
                                                                                WHEN sigarrtyp.event_type_cod IN ('DT', 'DN') AND sigarrtyp.status_cod = 'ACT' THEN 'rule2_case2_caso2'
                                                                                ELSE NULL 
                                                                            END rule2_case2_caso2
                                                                    FROM be4_ndod.tbl_global_fiche sigarrtyp
                                                                        LEFT JOIN gbn_credit_risk_reporting.tbl_cpty_ref cpty ON (cpty.cd_raf = sigarrtyp.cod_raf AND cpty.key_partition = sigarrtyp.key_partition)
                                                                    WHERE sigarrtyp.key_partition = '202206010000'
                                                                ) a 
                                                            WHERE a.rule2_case2_caso2 IS NOT NULL
                                                        ) b 
                                                    WHERE b.rank_ = 1
                                                    ) rule2_case2_caso2 ON rule1.cod_raf = rule2_case2_caso2.cod_raf
                                            LEFT JOIN 
                                                    ( 
                                                    SELECT 
                                                            cpty.cd_raf,
                                                            cpty.cd_ufo_comm,
                                                            'rule2_case3_caso1' AS rule2_case3_caso1
                                                    FROM gbn_credit_risk_reporting.tbl_cpty_ref cpty 
                                                    WHERE cpty.key_partition = '202206010000'
                                                    AND cpty.cd_ufo_comm LIKE '%19'
                                                    ) rule2_case3_caso1 ON rule1.cod_raf = rule2_case3_caso1.cd_raf
                                            LEFT JOIN 
                                                    (
                                                    SELECT 
                                                            cpty.cd_raf,
                                                            cpty.cd_ufo_comm,
                                                            'rule2_case3_caso1' AS rule2_case4_caso1
                                                    FROM gbn_credit_risk_reporting.tbl_cpty_ref cpty 
                                                    WHERE cpty.key_partition = '202206010000'
                                                    AND cpty.cd_ufo_comm LIKE '%35'
                                                    ) rule2_case4_caso1 ON rule1.cod_raf = rule2_case4_caso1.cd_raf

                            ) ptr ON base.cod_tiers_r4 = ptr.cod_tiers_r4
) current_key_partition LEFT JOIN 
                                                            (                                               
                                                                SELECT cod_tiers_r4 AS cod_r4, phasys_type, phasys_type_Date
                                                                FROM be4_ndod.tbl_global_watch
                                                                WHERE key_partition = '202205310000'
                                                            ) previous_key_partition ON current_key_partition.cod_tiers_r4 = previous_key_partition.cod_r4