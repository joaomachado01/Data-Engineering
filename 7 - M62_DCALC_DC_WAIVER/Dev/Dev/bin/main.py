import sys
import time
from pyspark.sql import *
from pyspark.sql import SparkSession
import datetime
#from dateutil.relativedelta import relativedelta, SU
from pyspark import SparkContext
import io
from functions import *
from Audit_trail import *
import getpass
import os

########################################  MAIN - Test Integration ###########################################
if __name__ == '__main__':
    #after the upgrade this is required to call hdfs from the command line
    username = getpass.getuser()
    
    # if username.lower()[1] == 'u':
    os.environ["HADOOP_CONF_DIR"]="/etc/hadoop/conf"
    spark = (SparkSession.builder.appName('Build Waiver Table').enableHiveSupport().getOrCreate())
    sc = SparkContext.getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 5000)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.hive.convertMetastoreOrc","false")
    # reading Global param , generated by Xldeploy dictionary on /etc/Globalparams.conf 
    props={}
    props=ReadingGlobalParams()

    #Add to props the name of the current Job.
    if username.lower()[1] == 'u':
        props['cod_job']="UM62DCALCDCWAIVER"
    elif username.lower()[1] == 'b' :
        props['cod_job']="BM62DCALCDCWAIVER"
    elif username.lower()[1] == 'p':
        props['cod_job']="PM62DCALCDCWAIVER"
    print(props)
    props["monthly_indicators_file"]="./etc/tablesToLoad.csv"
    props["hive_db"]=props["hive_db_name_dest"]
    props["code_appli_dir"]=props["codeAppforDir"]

    tmpODAT = sys.argv[3]
    props = apply_activation_odat_j1(props , tmpODAT ) 

    props,tables,select_str=Read_table_and_code_indicators(props)
    print("### SQL QUERY ###")
    print(select_str)
    audit_schema = get_schema_empty_table_audit(spark)
    max_col_id = get_max_column_ID( spark, props['history_audit_tabel_name_Final'])

    #Insert 'Start_Job' in Hive Table
    table = tables[0]
    props["HiveTabel"] = table
    start_time   =  time.time()
    dicoTableMain = construct_tmp_values_for_audit_trail(start_time, None, props)
    max_col_id=write_audit_row_management( spark  , props , dicoTableMain  ,  'start_job' , audit_schema, max_col_id )



    for i,table in enumerate(tables):
        props["hivetable"] = table
        props["HiveTabel"] = table
        props["HiveTableForWorking"] =  str(props["hivetable"].split(".")[1])
        print(props["HiveTableForWorking"])
        print(type(props["HiveTableForWorking"]))
        
        dicoTable = construct_tmp_values_for_audit_trail( start_time , None , props  )
        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'start' , audit_schema, max_col_id )


        # Create query for target table if not exists
        max_col_id=Read_table_definitions_and_Create_table_If_Not_Exits(spark, props, dicoTable, props['table_def_file'], table, audit_schema, max_col_id)

        max_col_id = load_working(spark, select_str[i], props, dicoTable  , audit_schema, max_col_id )
        max_col_id = load_final_table(spark, props, dicoTable  , audit_schema, max_col_id )

        max_col_id = write_audit_row_management( spark  , props , dicoTable  ,  'finish' , audit_schema, max_col_id )
    #Insert 'Finish_Job' in Hive Table
    table = tables[len(tables)-1]
    props["HiveTabel"] = table
    dicoTableMain['cod_file'] = props['HiveTabel']
    max_col_id = write_audit_row_management( spark  , props , dicoTableMain  ,  'finish_job' , audit_schema, max_col_id  )