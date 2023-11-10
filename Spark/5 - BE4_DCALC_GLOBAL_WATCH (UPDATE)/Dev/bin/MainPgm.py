"""
|==================================================|
|         CODE Name: MainPgmBPCETables.py          |
|   PURPOSE: Main code to load output tables       |
|            for BPCE purposes                     |
|=========|==========|====================|========|
| Version |    Date  |       author       |        |
|   v1.0  | 20210326 | Guilherme TEIXEIRA | Amexio |
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
    spark = (SparkSession.builder.appName('LOADING Output BPCE tables HADOOP_HIVE - Risques').enableHiveSupport().getOrCreate())
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark.conf.set("spark.sql.crossJoin.enabled", True) #necessary because of our query complexity
    sc = SparkContext.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # props = {'sep':';','encoding_in':'iso-8859-1','TrueNameOfPartkey':'partition_key' ,'path_hdfs_dbtable':'hdfs://hdfsdwb/dr/m62/public/hive/m62_great.db/','filepath':'/dr/m62/public/hive/files_bpce_weekly/','date_col':'date_arrete','hive_db_name_dest':'m62_great'}
    props = ReadingGlobalParams()
    #Add to props the name of the current Job.
    if username.lower()[1] == 'u':
        props['cod_job']="UBE4DCALCGLOBALWATCH"
    elif username.lower()[1] == 'b' :
        props['cod_job']="BBE4DCALCGLOBALWATCH"
    elif username.lower()[1] == 'p':
        props['cod_job']="PBE4DCALCGLOBALWATCH"

    props['table_def_file']='./etc/table_definitions.csv'
    props['table_ins_file']='./etc/table_inserts.csv'
    # param_table='m62_great.tbl_appli_param'
    # param_dict = get_param_dict(spark, param_table)
    # print(type(props),type(param_dict))
    # print(props)
    # print(param_dict)
    # props.update(param_dict)

    print(props)    

    # props['TrueNameOfPartkey']='partition_key'

    tmpODAT = sys.argv[3]
    # tmpODAT = '201231'
    # tmpODAT = '210101'
    # tmpODAT = transform_into_friday(tmpODAT)
    props = apply_activation_odat_j1(props , tmpODAT )
    # props['partition_to_out']=get_partition(tmpODAT)
    props['partition_to_out'] = props['ODAT']
    props['partition_to_load'] = props['partition_to_out']
    # if props['partition_to_load']=='':
    #     print('There are no partitions to load!')
    #     pass
    tables = Read_Table_Names(sys.argv[2])
    #props['history_audit_tabel_name'] = 'tbl_audit_trail'
    props['history_audit_tabel_name_Final'] = props['hive_db_name_dest']+'.'+props['history_audit_tabel_name']
    audit_schema = get_schema_empty_table_audit(spark)
    max_col_id = get_max_column_ID( spark, props['history_audit_tabel_name_Final'])
    start_time = time.time()
    props['final_table_name'] = props['hive_db_name_dest']+'.'+tables[0]
    dicoTableMain = construct_tmp_values_for_audit_trail(start_time, None, props)
    dicoTableMain[props['TrueNameOfPartkey']] = props['partition_to_load']
    ##############################   START JOB ##############################
    max_col_id = write_audit_row_management(spark, props, dicoTableMain, 'start_job', audit_schema, max_col_id)
    props['crisisdate']=''
    for table in tables:
        start_time = time.time()
        props['final_table_name'] = props['hive_db_name_dest']+'.'+table
        props['partitions_and_tables_for_audit'] = ''
        dicoTable = construct_tmp_values_for_audit_trail( start_time , None , props  )
        ##############################   START ##############################
        dicoTable[props['TrueNameOfPartkey']] = props['partition_to_load']
        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'start'  ,audit_schema, max_col_id)
        create_query = Read_table_definitions(props['table_def_file'], table,props)
        print('TABLE: ',table, 'QUERY: ',create_query)
        spark.sql(create_query)
        # max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'Read table definitions',audit_schema, max_col_id  )
        ins_query, join_tables, type_table = Read_insert_parameters(props['table_ins_file'],table,props,spark)
        primary_part='201912310000'
        # print(ins_query)
        props['partitions_and_tables_for_audit']=props['partitions_and_tables_for_audit']+'['
        for i, join_table in enumerate(join_tables):
            str_part,primary_part = determine_partition_date_alt(spark,props,join_table,primary_part,type_table[i])
            print('TABLE: ',table,' JOIN_TABLE: ',join_table,' PARTITION: ',str_part)
            props['partitions_and_tables_for_audit']=props['partitions_and_tables_for_audit']+join_table+'->'+str_part[0:8]+','
            q_part=str_part[0:4]+'-'+str_part[4:6]+'-'+str_part[6:8]
            ins_query=ins_query.replace('PARTXXXXXXX'+str(i+1),str_part).replace('PARTQQQQQQQ'+str(i+1),q_part).replace('SETCRISISDATE',props['crisisdate'])
            if str_part=='BREAK':
                print('Missing Partitions!')
                break
        # print(ins_query)
        props['partitions_and_tables_for_audit']=props['partitions_and_tables_for_audit'][:-1]+']'
        # max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'Built insert statement', audit_schema, max_col_id  )
        # max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'Table created if not exist', audit_schema, max_col_id  )
        print('TABLE: ',table, 'QUERY: ',ins_query)
        spark.sql(ins_query) 
        new_lines=spark.sql('Select count(1) as cnt FROM '+props['hive_db_name_dest']+'.'+table+' where '+props['TrueNameOfPartkey']+'='+props['partition_to_load'])
        dicoTable[props['TrueNameOfPartkey']]=props['partition_to_load']
        dicoTable['df'] = int(new_lines.select('cnt').collect()[0].cnt)
        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'insert_partition', audit_schema, max_col_id  )
        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'Partitions Loaded', audit_schema, max_col_id  )        
        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'finish', audit_schema, max_col_id  )
    dicoTableMain['cod_file'] = props['hive_db_name_dest']+'.'+tables[-1]
    max_col_id = write_audit_row_management( spark  , props , dicoTableMain  ,  'finish_job' , audit_schema, max_col_id )