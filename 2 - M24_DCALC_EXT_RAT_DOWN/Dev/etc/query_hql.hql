WITH getting_cod_note_m12_f1_last_date AS 
(

    SELECT 
        DISTINCT(last_date_f1.date_minus_12) AS date_12
        FROM (
                SELECT 
                     CASE 
                        WHEN date_format(add_months(cast(to_date(from_unixtime(unix_timestamp(substr(notation.key_partition,1,8), 'yyyyMMdd'))) as date), -12), 'u') = 7 THEN concat(regexp_replace(date_sub(add_months(cast(to_date(from_unixtime(unix_timestamp(substr(notation.key_partition,1,8), 'yyyyMMdd'))) as date), -12), 2), "-", ""), "0000")
                        WHEN date_format(add_months(cast(to_date(from_unixtime(unix_timestamp(substr(notation.key_partition,1,8), 'yyyyMMdd'))) as date), -12), 'u') = 6 THEN concat(regexp_replace(date_sub(add_months(cast(to_date(from_unixtime(unix_timestamp(substr(notation.key_partition,1,8), 'yyyyMMdd'))) as date), -12), 1), "-", ""), "0000")
                        ELSE concat(regexp_replace(add_months(cast(to_date(from_unixtime(unix_timestamp(substr(notation.key_partition,1,8), 'yyyyMMdd'))) as date), -12), "-", ""), "0000")
                     END AS date_minus_12
                FROM m24_trr.tbl_ta_notation notation 
                WHERE notation.key_partition = 'CURRENT_KP'   
             ) last_date_f1
),
external_rating_rafs AS 
(
    SELECT DISTINCT(notation.cod_tiers_r4) AS cod_rafs 
    FROM m24_trr.tbl_ta_notation notation
    WHERE (notation.key_partition in ('CURRENT_KP', 'PREVIOUS_KP') OR notation.key_partition = (SELECT * FROM getting_cod_note_m12_f1_last_date)) and 
                                        (
                                            (notation.cod_emetteur = 'SP' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                            (notation.cod_emetteur = 'MO' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                            (notation.cod_emetteur = 'FI' and notation.cod_typ_not_int in ('LTR', 'LTS', 'IR'))
                                        )
), 
rating_params AS 
(
    SELECT 
        ref.key_partition, 
        ref.cod_param, 
        ref.cod_val, 
        ref.cod_val_2, 
        ref.cod_val_3 
    FROM m62_great.tbl_rep_ref_param ref
    WHERE ref.cod_param = 'NOTE_RANG_SQR'
), 
getting_tiers_cod_codif_ext AS 
(
    SELECT 
        DISTINCT ext.cod_codif_ext, 
        ext.cod_tiers_r4 
    FROM m24_trr.tbl_ta_tiers_codif_ext ext
    WHERE ext.key_partition = 'CURRENT_KP' AND ext.cod_typ_codif = 'RAF' AND ext.cod_tiers_r4 IN (
                                                                                       SELECT * 
                                                                                       FROM external_rating_rafs
                                                                                    )
),
getting_cod_note_j AS
(
    SELECT * 
    FROM (
            SELECT 
                notation.cod_tiers_r4, 
                notation.cod_emetteur AS cod_agence, 
                notation.cod_typ_not_int AS cod_type_note, 
                notation.cod_val_noti AS cod_note_j, 
                notation.dat_deb_nota AS dat_notation_j, 
                notation.num_notation, 
                RANK() OVER(PARTITION BY notation.cod_tiers_r4, notation.cod_emetteur, notation.cod_typ_not_int ORDER BY notation.cod_val_noti DESC, notation.num_notation DESC) AS rank_,
                notation.cod_outlook, 
                notation.dat_outlook, 
                notation.dat_deb_nota,
                notation.key_partition
            FROM m24_trr.tbl_ta_notation notation WHERE key_partition = 'CURRENT_KP' AND (
                                                                                            (notation.cod_emetteur = 'SP' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                                                                            (notation.cod_emetteur = 'MO' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                                                                            (notation.cod_emetteur = 'FI' and notation.cod_typ_not_int in ('LTR', 'LTS', 'IR'))
                                                                                         ) 
        ) querie 
    WHERE querie.rank_ = 1
),
getting_cod_note_j1 AS 
(
    SELECT * 
    FROM (
            SELECT 
                notation.cod_tiers_r4, 
                notation.cod_emetteur AS cod_agence, 
                notation.cod_typ_not_int AS cod_type_note, 
                notation.cod_val_noti AS cod_note_j1, 
                notation.dat_deb_nota AS dat_notation_j1, 
                notation.num_notation, 
                RANK() OVER(PARTITION BY notation.cod_tiers_r4, notation.cod_emetteur, notation.cod_typ_not_int ORDER BY notation.cod_val_noti DESC, notation.num_notation DESC) AS rank_
            FROM m24_trr.tbl_ta_notation notation WHERE key_partition = 'PREVIOUS_KP' AND (
                                                                                            (notation.cod_emetteur = 'SP' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                                                                            (notation.cod_emetteur = 'MO' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                                                                            (notation.cod_emetteur = 'FI' and notation.cod_typ_not_int in ('LTR', 'LTS', 'IR'))
                                                                                         ) 
        ) querie 
    WHERE querie.rank_ = 1
), 
getting_cod_note_m12_f1 AS 
(
    SELECT * 
    FROM (
            SELECT 
                notation.cod_tiers_r4, 
                notation.cod_emetteur AS cod_agence, 
                notation.cod_typ_not_int AS cod_type_note, 
                notation.cod_val_noti AS cod_note_m12, 
                notation.dat_deb_nota AS dat_notation_m12, 
                notation.num_notation, 
                RANK() OVER(PARTITION BY notation.cod_tiers_r4, notation.cod_emetteur, notation.cod_typ_not_int ORDER BY notation.cod_val_noti DESC, notation.num_notation DESC) AS rank_, 
                notation.key_partition
            FROM m24_trr.tbl_ta_notation notation 
            WHERE notation.key_partition = (SELECT last_date.date_12 FROM getting_cod_note_m12_f1_last_date last_date)
            AND (
                    (notation.cod_emetteur = 'SP' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                    (notation.cod_emetteur = 'MO' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                    (notation.cod_emetteur = 'FI' and notation.cod_typ_not_int in ('LTR', 'LTS', 'IR'))
                ) 
        ) querie 
    WHERE querie.rank_ = 1
), 
notation_base AS
(

    SELECT 
            DISTINCT
            notation.cod_tiers_r4,
            notation.cod_emetteur,
            notation.cod_typ_not_int
    FROM m24_trr.tbl_ta_notation notation
    WHERE (notation.key_partition in ('CURRENT_KP', 'PREVIOUS_KP') OR notation.key_partition = (SELECT * FROM getting_cod_note_m12_f1_last_date)) and 
                                        (
                                            (notation.cod_emetteur = 'SP' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                            (notation.cod_emetteur = 'MO' and notation.cod_typ_not_int in ('DLT', 'CTP')) or 
                                            (notation.cod_emetteur = 'FI' and notation.cod_typ_not_int in ('LTR', 'LTS', 'IR'))
                                        )
),
final_querie_main_f1 AS 
(
    SELECT 
        DISTINCT tiers.cod_codif_ext AS cod_raf, 
        notation.cod_tiers_r4 AS cod_tiers_r4, 
        notation.cod_emetteur AS cod_agence, 
        notation.cod_typ_not_int AS cod_type_note, 
        j.cod_note_j, 
        j1.cod_note_j1, 
        m12.cod_note_m12, 
        CAST(params_current.cod_val_3 AS INT) AS cod_rang_j, 
        CAST(params_d1.cod_val_3 AS INT) AS cod_rang_j1, 
        CAST(params_d12.cod_val_3 AS INT) AS cod_rang_m12, 
        j.cod_outlook AS cod_outlook, 
        j.dat_outlook AS dat_outlook, 
        j.dat_deb_nota AS dat_notation_j, 
        j1.dat_notation_j1, 
        m12.dat_notation_m12, 
        j.key_partition 
    FROM notation_base notation 
        LEFT JOIN getting_tiers_cod_codif_ext tiers ON notation.cod_tiers_r4 = tiers.cod_tiers_r4 
        LEFT JOIN getting_cod_note_j j ON (notation.cod_tiers_r4 = j.cod_tiers_r4 AND notation.cod_emetteur = j.cod_agence AND notation.cod_typ_not_int = j.cod_type_note)
        LEFT JOIN getting_cod_note_j1 j1 ON (notation.cod_tiers_r4 = j1.cod_tiers_r4 AND notation.cod_emetteur = j1.cod_agence AND notation.cod_typ_not_int = j1.cod_type_note) 
        LEFT JOIN getting_cod_note_m12_f1 m12 ON (notation.cod_tiers_r4 = m12.cod_tiers_r4 AND notation.cod_emetteur = m12.cod_agence AND notation.cod_typ_not_int = m12.cod_type_note) 
        LEFT JOIN (SELECT * FROM rating_params r WHERE r.key_partition = 'PREVIOUS_KP') params_current ON (params_current.cod_val = notation.cod_emetteur AND params_current.cod_val_2 = j.cod_note_j)  
        LEFT JOIN (SELECT * FROM rating_params r WHERE r.key_partition = 'PREVIOUS_KP') params_d1 ON (params_d1.cod_val = notation.cod_emetteur AND params_d1.cod_val_2 = j1.cod_note_j1) 
        LEFT JOIN (SELECT * FROM rating_params r WHERE r.key_partition = (SELECT * FROM getting_cod_note_m12_f1_last_date)) params_d12 ON (params_d12.cod_val = notation.cod_emetteur AND params_d12.cod_val_2 = m12.cod_note_m12)
)
SELECT 
    cod_raf, 
    cod_tiers_r4, 
    cod_agence, 
    cod_type_note, 
    cod_note_j, 
    cod_note_j1, 
    cod_note_m12, 
    cod_rang_j, 
    cod_rang_j1, 
    cod_rang_m12, 
    CASE 
        WHEN cod_rang_j1 <= 7 THEN '1' 
        WHEN (cod_rang_j1 >= 8 AND cod_rang_j1 <= 10) THEN '2' 
        WHEN cod_rang_j1 > 10 THEN '3' 
        ELSE NULL 
    END cod_stage_j1, 
    CASE 
        WHEN cod_rang_m12 <= 7 THEN '1' 
        WHEN (cod_rang_m12 >= 8 AND cod_rang_m12 <= 10) THEN '2' 
        WHEN cod_rang_m12 > 10 THEN '3' 
        ELSE NULL 
    END cod_stage_m12, 
    CASE 
        WHEN (cod_rang_j - cod_rang_j1) IS NOT NULL THEN CAST((cod_rang_j - cod_rang_j1) AS string) 
        ELSE NULL 
    END mnt_var_note_j1, 
    CASE 
        WHEN (cod_rang_j - cod_rang_m12) IS NOT NULL THEN CAST((cod_rang_j - cod_rang_m12) AS string) 
        ELSE NULL 
    END mnt_var_note_m12, 
    cod_outlook, 
    dat_outlook, 
    dat_notation_j, 
    dat_notation_j1, 
    dat_notation_m12, 
    CASE 
        WHEN key_partition IS NULL THEN 'CURRENT_KP'
        ELSE key_partition 
    END key_partition  
FROM final_querie_main_f1 
WHERE cod_tiers_r4 IS NOT NULL