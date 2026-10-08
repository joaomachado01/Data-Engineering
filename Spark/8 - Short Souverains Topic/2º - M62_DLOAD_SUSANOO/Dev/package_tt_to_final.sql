create or replace PACKAGE BODY PACK_REP_BATCH_SUS AS

  PROCEDURE alim_rep_sus AS
  
      v_step varchar2(3);
      v_partition varchar2(12);
      v_debut date;
      v_syst varchar2(5);
      v_count_part integer;

  BEGIN 

          -- Initialisation du système
          v_syst := 'SUS'; 

          -- Récupération de l'étape 
          SELECT SYSDATE INTO v_debut from dual;

          -- Ecriture d'une ligne d'audit 
          PACK_REP_BATCH.PISTE_AUDIT('I','MAJ table REP_SUS_SENSI','Debut procédure alim_rep_sus','',v_syst,null,null);
          COMMIT;

          -- Mise à jour du step   
          v_step := '0';

          -- Sélection de la partition
          SELECT PACK_REP_BATCH.CALC_ARRETE('SUS')||'0000' INTO v_partition FROM DUAL;

          -- Mise à jour du step   
          v_step := '1';

          -- Test de l'existence de la partition  
          SELECT COUNT(*) INTO v_count_part FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = 'REP_SUS_SENSI' AND PARTITION_NAME = 'P_'||v_partition;

          -- Ajout de la partition si nécessaire
          IF v_count_part=0 THEN 
            ADD_PARTITIONS_REP('REP_SUS_SENSI',v_partition); 
          -- Lancement de l'analyse de la nouvelle partition 
          --TODO     
          END IF;

          -- Mise à jour du step   
          v_step := '2';

          -- Suppression des données (pour éviter des doublons en cas de rechargement)
          DELETE REP_SUS_SENSI WHERE KEY_PARTITION=v_partition;
          COMMIT;

          -- Mise à jour du step   
          v_step := '3';   

          -- Insertion des nouvelles lignes       
          INSERT INTO rep_sus_sensi
          (
            KEY_PARTITION                   ,
            DAT_STOCK                       ,
            COD_SYST_SHORT_NAME             ,
            COD_PROVIDER                    ,
            TYP_SENSI                       ,
            SUB_TYPE_SENSI                  ,
            COD_CATE_SENSI                  ,
            COD_SUB_CATE_SENSI              ,
            COD_SCENARIO                    ,
            COD_SENSI_TRANSPARENCY          ,
            NUM_ID_CURVE                    ,
            COD_CLOSING                     ,
            COD_BOOK                        ,
            COD_INSTRUMENT                  ,
            NUM_COUNT_SENSI                 ,
            MOT_SENSI_EUR                   
          )
          SELECT 
            v_partition                      ,
            to_date(STOCK_DATE, 'YYYY-MM-DD'), 
            SYSTEM_SHORT_NAME                ,
            PROVIDER                         ,
            SENSI_TYPE                       ,
            SENSI_SUB_TYPE                   ,
            SENSI_CATEGORY                   ,
            SENSI_SUB_CATEGORY               ,
            SENSI_TRANSPARENCY               ,
            SCENARIO                         ,
            CURVE_ID                         ,
            CLOSING                          ,
            BOOK                             ,
            INSTRUMENT_ID                    , 
            CNT                              ,
            SENSI_EUR                        
          FROM rep_sus_tt_sensi;

          -- Nombre de lignes insérées  
          PACK_REP_BATCH.PISTE_AUDIT('I','MAJ table REP_SUS_SENSI','Lignes insérées','',v_syst,SQL%ROWCOUNT,null,v_partition);
          COMMIT;

          -- Ecriture d'une ligne d'audit 
          PACK_REP_BATCH.PISTE_AUDIT('I','MAJ table REP_SUS_SENSI','Fin procédure alim_rep_sus' ,'',v_syst,null,(sysdate-v_debut),v_partition);
          COMMIT;    

          EXCEPTION
          WHEN OTHERS THEN
          PACK_REP_BATCH.PISTE_AUDIT('E','Erreur ALIM_REP_SUS', null,v_step,v_syst,null,null,v_partition);
          COMMIT;
          --RAISE_APPLICATION_ERROR(-20099,'Problème dans la procédure PACK_REP_BATCH_NIE.alim_REP_NIE_055',False);
  END alim_rep_sus;

END PACK_REP_BATCH_SUS;