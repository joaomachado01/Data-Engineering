### IMP

"""
|==================================================|
|         CODE Name: Main.py                       |
|   PURPOSE: Main code to detect the occurrence of |       
| non-technical unpaid with financial institutions |
|=========|==========|====================|========|
| Version |    Date  |       author       |        |
|   v1.0  | 20220525 |    Joao Machado    | Amexio |
|=========|==========|====================|========|
"""

import sys
import pyspark
import pyspark.sql.functions
from pyspark.sql.functions import col, lit,concat
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from Functions import *
from Audit_trail import *
import getpass


if __name__ == "__main__":
    username = getpass.getuser()
    os.environ["HADOOP_CONF_DIR"]="/etc/hadoop/conf"
    spark = (SparkSession.builder.appName('Create table TBL_BREF_TRIGGERS').enableHiveSupport().getOrCreate())
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    sc = SparkContext.getOrCreate()
    props = ReadingGlobalParams()
    # Add to props the name of the current Job.
    if sys.argv[4] == 'P':
      if username.lower()[1] == 'u':
        props['cod_job']="UL97DCALCDETAILIMP"
      elif username.lower()[1] == 'b' :
        props['cod_job']="BL97DCALCDETAILIMP"
      elif username.lower()[1] == 'p':
        props['cod_job']="PL97DCALCDETAILIMP"
    else:
      if username.lower()[1] == 'u':
        props['cod_job']="UL97DCALCDETAILIMPMENS"
      elif username.lower()[1] == 'b' :
        props['cod_job']="BL97DCALCDETAILIMPMENS"
      elif username.lower()[1] == 'p':
        props['cod_job']="PL97DCALCDETAILIMPMENS"
    print(props) 
    props['TrueNameOfPartkey']='partition_key'
    tmpODAT = sys.argv[3]
    # test if (ODAT==j-1) is activated : check XLD dico
    props   = apply_activation_odat_j1(props , tmpODAT )    
    props['partition_to_out']=props['ODAT'] 
    #props['partition_to_load']=determine_partition_date(spark,props)
    tables=Read_Table_Names(sys.argv[2])
    print("############################################################################# TABLES #############################################################################")
    print(tables)
    props['history_audit_tabel_name_Final'] = props['hive_db_name_dest']+'.'+props['history_audit_tabel_name']
    audit_schema = get_schema_empty_table_audit(spark)
    max_col_id = get_max_column_ID( spark, props['history_audit_tabel_name_Final'])
    start_time   =  time.time()
    props['final_table_name'] = tables[0]
    #Insert 'Start_Job' in Hive Table
    table = tables[0]
    props['HiveTabel']       = "{0}.{1}_{2}".format(props['hive_db_name_dest'],'tbl',table.rstrip()).lower()
    dicoTableMain = construct_tmp_values_for_audit_trail( start_time , None , props)
    dicoTableMain[props['TrueNameOfPartkey']]=props['partition_to_out']
    ##############################   START JOB ##############################
    max_col_id = write_audit_row_management( spark  , props , dicoTableMain  ,  'start_job' , audit_schema, max_col_id )
    for table in tables:
        last_kps = Update_Last_2KP(spark, props, table)
        itr = 0
        for key_partition in last_kps:
            query_itr = 'query'+str(itr+1)
            ##############################   START ##############################
            props['HiveTabel']       = "{0}.{1}_{2}".format(props['hive_db_name_dest'],'tbl',table.rstrip()).lower()
            start_time   =  time.time()
            props['final_table_name'] = table
            dicoTable = construct_tmp_values_for_audit_trail( start_time , None , props  )
            dicoTable[props['TrueNameOfPartkey']]=key_partition
            max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'start'  ,audit_schema, max_col_id)
            
            # Create query for target table
            create_query, create_query_temp=Read_table_definitions(props['table_def_file'], table,props)
            
            # Execute create query
            print("##################################################################################### CREATE TABLE #####################################################################################") 
            print('TABLE: ',table, 'QUERY: ',create_query)
            spark.sql(create_query)
            print("##################################################################################### CREATE TABLE #####################################################################################") 
            
            # Insert query for target table
            ins_query, ins_rows, props, delete_statement1, delete_statement2 = Read_insert_parameters(spark, props['table_ins_file'],table,props, create_query_temp, props['query_hql'], key_partition, query_itr)
            
            # Execute insert query
            print('TABLE: ',table, 'QUERY: ',ins_query)
            spark.sql(ins_query) 
            
            # Delete temporary table
            DeleteTemporaryTable(spark, delete_statement1, delete_statement2)
            
            dicoTable[props['TrueNameOfPartkey']]=key_partition
            dicoTable['df'] = ins_rows
            max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'insert_partition', audit_schema, max_col_id  )
            max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'Partitions Loaded', audit_schema, max_col_id  )       
            ##############################   FINISH ############################## 
            max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'finish', audit_schema, max_col_id  )
            itr = itr + 1
            print("### ITERATION ####")
            print(itr)
    ##############################   FINISH JOB ##############################
    table = tables[-1]
    props['HiveTabel']       = "{0}.{1}_{2}".format(props['hive_db_name_dest'],'tbl',table.rstrip()).lower()
    dicoTableMain['cod_file'] = props['HiveTabel']
    max_col_id = write_audit_row_management( spark  , props , dicoTableMain  ,  'finish_job' , audit_schema, max_col_id )

