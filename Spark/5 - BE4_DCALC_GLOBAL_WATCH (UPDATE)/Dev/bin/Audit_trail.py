import os
import sys
import calendar
import time
import datetime
import pyspark.sql.functions as F

from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField , FloatType, TimestampType
from pyspark.sql.functions import col, udf
from pyspark.sql import *  
from pyspark.sql.functions import substring , to_timestamp , to_date , unix_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, dayofmonth, hour, dayofyear, month, year, weekofyear, date_format

from dateutil.relativedelta import relativedelta, SU


def create_empty_table_if_not_exists(spark , props  ):
    """
    this function creates an empty an external table if not exist
    """
    # schema
    schema = StructType([
    StructField('COD_LOG', IntegerType(), False),
    StructField('FLG_LOG', StringType(), True),
    StructField('COD_TREATMENT', StringType(), True),
    StructField('VLS_MESSAGE', StringType(), True),
    StructField('KEY_PARTITION', StringType(), True),
    StructField('COD_FILE', StringType(), True),
    StructField('COD_JOB', StringType(), True),
    StructField('DTT_CREATION', TimestampType(), True),
    StructField('INT_LINES', StringType(), True),
    StructField('INT_SEC', IntegerType(), True),
    StructField('DTT_CHARGEMENT', TimestampType(), True),
    StructField('INT_REJECT_LINE', IntegerType(), True),
    StructField('TIME_KEY', StringType(), True)    ])
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    df = df.toDF(*[c.lower() for c in df.columns])
    if (spark.sql("show tables in  " + props['hive_db_name_dest'] ).filter(col("tableName") == props['history_audit_tabel_name'].lower()).count() > 0):
        print('table audit ', props['history_audit_tabel_name_Final'] ,' exist !')
        return df
    else:
        path_audit_trail = props['Path_hdfs_db'] + props['history_audit_tabel_name']
        df.write.format("orc").option("encoding",'utf-8').option("path", path_audit_trail  ).saveAsTable(props['history_audit_tabel_name_Final'])
        print('table audit ', props['history_audit_tabel_name_Final'] ,' created !')
        quafos = " ALTER TABLE {0} SET TBLPROPERTIES ( 'auto.purge'='true'  )  ".format(props['history_audit_tabel_name_Final'])
        spark.sql(quafos)
        quafos = ""
        return df

def get_schema_empty_table_audit(spark):
    # schema TRY TO JUST HAVE SCHEMA
    schema = StructType([
    StructField('cod_log', IntegerType(), False),
    StructField('flg_log', StringType(), True),
    StructField('cod_treatment', StringType(), True),
    StructField('vls_message', StringType(), True),
    StructField('cod_file', StringType(), True),
    StructField('cod_job', StringType(), True),
    StructField('dtt_creation', TimestampType(), True),
    StructField('int_lines', StringType(), True),
    StructField('int_sec', IntegerType(), True),
    StructField('dtt_chargement', TimestampType(), True),
    StructField('int_reject_line', IntegerType(), True),    
    StructField('key_partition', StringType(), True),
    StructField('time_key', StringType(), True)])
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    #df = df.toDF(*[c.lower() for c in df.columns])
    return df.schema

def get_exec_time(start_time):
    """
    Get execution time and round results to 4 digits post commas
    """
    import time
    return round((time.time() - start_time ),4)

def get_max_column_ID(spark, history_audit_tabel_name):
    """
    get max columns id depending to cod_log columns in tbl_audit_trail
    """
    query = "( SELECT MAX(cod_log) FROM {0} )".format(history_audit_tabel_name)
    df = spark.sql(query)
    TT = df.rdd.map(lambda row : row[0]).collect()[0]
    if TT:
        return TT
    else:
        return 0

#dicoTable = value of PK, of tmp_tab_cod_file
def enrichir_dico_variable_values( spark, props , dicoTable , traitement, max_col_id   ):
    max_col_id = max_col_id+1
    dico_df_Values = {}
    # datetime object containing current date and time
    dico_df_Values["dtt_chargement"] =  datetime.datetime.now()
    dico_df_Values["time_key"] =  str(dico_df_Values["dtt_chargement"])[0:10].replace("-","") #FOR PARTITION PURPOSES
    # dico_df_Values['cod_log']        =  get_max_column_ID( spark, props['history_audit_tabel_name_Final']) + 1
    dico_df_Values['cod_log']        =  max_col_id
    dico_df_Values['key_partition']  =  dicoTable[props['TrueNameOfPartkey']]
    dico_df_Values['cod_file']       =  dicoTable['cod_file']
    dico_df_Values['cod_job']       =  dicoTable['cod_job']
    dico_df_Values["int_sec"]        =  int(get_exec_time( dicoTable['start_time'] ))
    dico_df_Values["dtt_creation"]  =  datetime.datetime.now() 
    dico_df_Values["int_lines"]     =  dicoTable['df']
    if traitement =='insert_partition':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'insert_partition_error':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'feed_tab_with_partition':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"] = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        return dico_df_Values, max_col_id
    elif traitement == 'create_tab_with_partition_error':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"] = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'drop_business_table':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"] = "E"
        dico_df_Values["cod_treatment"] = "DT"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'error_while_writing_audit_row':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'add_partition_to_file':  
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'add_partition_if_not_exists_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_addCols':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement + ' ' + str(dicoTable["vls_message"])
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_noColsAdded':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_addCols_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_DeleteCols':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'Columns_Deleted':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement + ' ' + str(dicoTable['vls_message'])
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_No_Column_Deleted':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'comparing_schema_DeleteCols_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'partition_exists_dropped':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "I"
        dico_df_Values["cod_treatment"] = "DP"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'drop_partition_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]     = "E"
        dico_df_Values["cod_treatment"] = "DP"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'insert_new_partition':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'insert_new_partition_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'create_tab_with_partition_ko':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "E"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"] = traitement 
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'drop_table_ok':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "DT"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement == 'drop_table_ko__not_found':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "E"
        dico_df_Values["cod_treatment"] = "DT"
        dico_df_Values["vls_message"] = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='start':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "S"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='finish':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "S"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='start_job':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "S"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='finish_job':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "S"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='1st_creation_of_pre_partitionned_table':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='1st_creation_of_non_pre_partitionned_table':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "C"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='Consistency Control OK':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id
    elif traitement =='Consistency Control KO':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "E"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = traitement
        dico_df_Values["int_reject_line"] = None
        return dico_df_Values, max_col_id        
    elif traitement =='Partitions Loaded':
        print('# traitement type ', traitement)
        dico_df_Values["flg_log"]       = "I"
        dico_df_Values["cod_treatment"] = "T"
        dico_df_Values["vls_message"]   = props['partitions_and_tables_for_audit']
        dico_df_Values["int_reject_line"] = None        
        return dico_df_Values, max_col_id

def write_audit_row_management(spark  , props , dicoTable  ,  traitement, audit_schema, max_col_id   ):
    try:
        dico_df_Values_tmp, max_col_id = enrichir_dico_variable_values( spark,  props , dicoTable ,  traitement, max_col_id   )
        cast_dico_to_df_and_insert_row_as_df(  spark , dico_df_Values_tmp  , props , traitement,audit_schema )
        return max_col_id
    except Exception as Excp:
        traitement = 'error_while_writing_audit_row'
        print(traitement  , Excp )
        dico_df_Values_tmp, max_col_id = enrichir_dico_variable_values( spark,  props , dicoTable ,  traitement, max_col_id  )
        cast_dico_to_df_and_insert_row_as_df( spark , dico_df_Values_tmp , props , traitement, audit_schema  )
        return max_col_id

def cast_dico_to_df_and_insert_row_as_df( spark , dico_df_Values  , props , traitement, audit_schema ):
    """
    This fuction cast a row present in dictionary to a dataframe with 1 row, to append to existing table ( tbl_audit_trail )
    """
    df = spark.createDataFrame([dico_df_Values] ,audit_schema )
    df.write.format('orc').mode('append').option("encoding",'utf-8').option("path",  props['Path_hdfs_db'] + props['history_audit_tabel_name'] ).insertInto(props['history_audit_tabel_name_Final'])
    # ins_query='INSERT INTO TABLE '+props['history_audit_tabel_name_Final']+' VALUES('+str(dico_df_Values['cod_log'])+',\''+str(dico_df_Values['flg_log'])+'\',\''\
    # +str(dico_df_Values["cod_treatment"])+'\',\''+str(dico_df_Values["vls_message"])+'\',\''+str(dico_df_Values['key_partition'])+'\',\''+str(dico_df_Values['cod_file'])+'\','+\
    # 'CAST(\''+str(dico_df_Values["dtt_creation"])+'\' AS TIMESTAMP)'+',\''+str(dico_df_Values["int_lines"])+'\','+str(dico_df_Values["int_sec"]).replace('None','NULL')+','+'CAST(\''+str(dico_df_Values["dtt_chargement"])+'\' AS TIMESTAMP)'+','+ str(dico_df_Values["int_reject_line"]).replace('None','NULL')+')'
    # print(ins_query)
    # spark.sql(ins_query)
 
    print(' row inserted on table audit : ', props['history_audit_tabel_name_Final'] , 'type traitement : ' + str(traitement ))

def inspect_RDMBS_vs_HIVE_row_numbers( count_RDBMS_of_this_partition , count_HIVE_toBeInsered ):
    if count_RDBMS_of_this_partition == count_HIVE_toBeInsered:
        return 'Consistency Control OK'
    else:
        return 'Consistency Control KO'