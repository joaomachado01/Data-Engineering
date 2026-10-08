### BREF

import os
import sys
import calendar
import time
import datetime
import io as io
# from datetime import datetime, date, timedelta
from pyspark.sql.functions import col, isnan, when, trim, udf, year, month, dayofmonth, format_number,hour,dayofyear,weekofyear,date_format, to_timestamp, unix_timestamp, substring, to_date
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField , FloatType, TimestampType, DoubleType
from pyspark.sql import *  
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
import pyspark.sql.functions as SF

def ReadingGlobalParams():
    """
    This function read Global parameters given in 1st position of spark submit command , it returns all param generated from XLDeploy into Host server
    """
    props = {}
    with open(sys.argv[1], "r") as f:
    # with open("/sluafrm62dwb1/appli/M62/M62_DLOAD_GREAT/etc/Globalparams.conf", "r") as f:
        for line in f:
            (key, val) = line.split("==")
            props[key] = str(val.rstrip()) 
    #read tables from param file
    print("Global Params Read from XLDeploy dictionary : ", props)
    props['history_audit_tabel_name_Final'] = props['hive_db_name_dest']+'.'+props['history_audit_tabel_name']
    props = clean_dico_XLD_From_quotes(props)
    return props

def clean_dico_XLD_From_quotes(props):
    for k,v in props.items():
        if props[k][-1]=='"' and props[k][0]=='"' :
            props[k] = props[k].replace('"', '')
    return props

def construct_tmp_values_for_audit_trail( start_time , dfCount , props  ):
    dicoTable = {}
    #dicoTable[props['TrueNameOfPartkey']] = str(props['ODAT'])
    dicoTable[props['TrueNameOfPartkey']] = ''
    dicoTable['cod_file']                 =  props['final_table_name']
    dicoTable['cod_job']                 =  props['cod_job']
    dicoTable['start_time'] = start_time
    dicoTable['df'] = dfCount
    return dicoTable

def Read_Table_Names_from_Config_File():
    """
    This function reads file given in 2nd position of spark submit command to read table to load into Hive
    """
    with open(sys.argv[2]) as f:
        tables = f.readlines()
    table_names = []
    for row in tables:
        table_names.append(row.split(',')[0].lower())
    print("filenames set as input : ", table_names)
    return table_names

def get_param_dict(spark,param_table):
    param_df = spark.sql('select * from '+param_table)
    df_dict = param_df.rdd.map(lambda row: {row[0]: row[1]}).collect()
    df_dict = merge_list_of_dict(df_dict)
    return df_dict

def merge_list_of_dict(list_of_dicts):
    return dict(i.items()[0] for i in list_of_dicts)

def Read_Table_Names(table_name_file):
    with open(table_name_file) as f:
        tables = f.readlines()
    table_names = []
    for row in tables:
        table_names.append(row.split(';')[0].lower())
    print("filenames set as input : ", table_names)
    return table_names    


def Read_insert_parameters(spark, insert_param_file,table,props, turnovers, create_query_temp, query_hql):
    """
    Function reads query parameters from file.
    """
    # Read turnover accounts
    with io.open(turnovers, encoding="utf-8") as f:
        params2 = f.readlines()
    for row2 in params2:
        turnovers = row2.split(';')
    turnovers = ', '.join(turnovers)

    # Get sql querie statement from hql file
    hql = ''
    f = open(query_hql, mode='r')
    for line in f.readlines():
      hql=hql+line.replace('\n',' ').split('--')[0]
    print(type(hql))
    
    # Read insert.csv file 
    with io.open(insert_param_file, encoding="utf-8") as f:
        params = f.readlines()
    insert = []

    for row in params:
        param = row.split(';')
        if param[0]==table:
          if Hive_Target_Table_Empty(spark, table, props) == 'Yes': # (tabel target empty = yes)
            print("######################## There's NO data in target table ########################")
            insert_rows_count = ''
            select = str(param[2]) 
               
            # Get max_kp for detail_monthly table
            kp_max = GetKPList(spark, props, 'tbl_ta_bref_fiche_detail_monthly')
            kp_max = "'"+kp_max[-1]+"'"
            print(kp_max)
              
            insert = hql.replace('KP', props['partition_to_out']).replace('turnovers', turnovers).replace('select_to_do', select).replace('max_kp_below_odate', "'201901010000'").replace('kp_max', kp_max)
            print(insert)
            df=spark.sql(insert)
            df.createTempView('query')
            insert = 'INSERT OVERWRITE TABLE '+props['hive_db_name_dest']+'.'+param[0]+' SELECT * FROM query'
            print(insert)
            # Get Inserted Rows Count
            spark.conf.set("spark.sql.broadcastTimeout",  36000) # to be able to read the count 
            insert_rows_count = ' SELECT COUNT(*) AS insert_rows_count FROM query'
            insert_rows_count = spark.sql(insert_rows_count)
            print("################# DF ########################") 
            insert_rows_count.show() 
            insert_rows_count = int(insert_rows_count.select('insert_rows_count').collect()[0].insert_rows_count)
            delete_statement1 = ''
            delete_statement2 = ''
            
            # get partitions for message in audit trail
            props['partitions_for_audit']=props['partition_to_out']
                   
          else:
              print("######################## There's data in target table ########################")
              
              # Creating the insert statement
              spark.conf.set("spark.sql.debug.maxToStringFields", 1000) # in order for spark to analyse the full querie because it was a long querie
              select = str(param[3]) 
              
              # Get max_kp_below_odate for bref_triggers table
              max_kp_below_odate = GetKPList(spark, props, table)
              max_kp_below_odate = Get_MAX_KP_Below_Odate(max_kp_below_odate, props)
              print(max_kp_below_odate)

            
              # Get max_kp for detail_monthly table
              kp_max = GetKPList(spark, props, 'tbl_ta_bref_fiche_detail_monthly')
              kp_max = "'"+kp_max[-1]+"'"
              print(kp_max)
              
              insert = hql.replace('KP', props['partition_to_out']).replace('turnovers', turnovers).replace('select_to_do', select).replace('max_kp_below_odate', max_kp_below_odate).replace('kp_max', kp_max)
              print(insert)
              df=spark.sql(insert)
              df.createTempView('query3')
              # Creating a temporary partitioned table to do the overwrite to the final table (from the temporary table query3)
              print("######################## QUERYYYY ########################")
              print(create_query_temp)
              print("######################## QUERYYYY ########################")
              spark.sql(create_query_temp)
              insert_statement = 'INSERT OVERWRITE TABLE '+props['hive_db_name_dest']+'.'+param[0]+'_temp'+' SELECT * FROM query3'
              print("######################## INSERT ########################")
              print(insert_statement)
              print("######################## INSERT ########################")
              spark.sql(insert_statement) 
              
              # Insert the data in the final table from the partitioned temporary table 
              insert = 'INSERT OVERWRITE TABLE '+props['hive_db_name_dest']+'.'+param[0]+' SELECT * FROM '+props['hive_db_name_dest']+'.'+param[0]+'_temp'
              print(insert)
              
              # Prepare Delete temporary partitioned table statement
              delete_statement1 = 'ALTER TABLE '+props['hive_db_name_dest']+'.'+param[0]+'_temp'+' SET TBLPROPERTIES ("external.table.purge"="True")'
              delete_statement2 = 'DROP TABLE '+props['hive_db_name_dest']+'.'+param[0]+'_temp'
              
              # get partitions for message in audit trail
              props['partitions_for_audit']=props['partition_to_out']
              
              # Get Inserted Rows Count
              spark.conf.set("spark.sql.broadcastTimeout",  36000) # to be able to read the count 
              insert_rows_count = ' SELECT COUNT(*) AS insert_rows_count FROM query3'
              insert_rows_count = spark.sql(insert_rows_count)
              insert_rows_count = int(insert_rows_count.select('insert_rows_count').collect()[0].insert_rows_count) 
        else:
          print("################### Its in the Break condition ##################")
          break
    return insert, insert_rows_count, props, delete_statement1, delete_statement2
    

def Hive_Target_Table_Empty(spark, table, props):
    df=spark.sql('SELECT COUNT(*) AS rows FROM '+props['hive_db_name_dest']+'.'+table)
    rows = int(df.select('rows').collect()[0].rows)
    if rows == 0:
      value = 'Yes'
    else:
      value = 'No'
    return value
    
def Odate_In_Source_Table(spark, props, table):
    df=spark.sql('SHOW PARTITIONS '+props['hive_db_name_dest']+'.'+table) 
    df=df.select(substring('partition',15,26).alias('key_partitions_only_in_source'))
    kps_list = df.select('key_partitions_only_in_source').collect()
    print(kps_list)
    kps_list = [str(row.key_partitions_only_in_source) for row in kps_list]
    print(kps_list)
    # Checking if ODATE is in the list
    if props['partition_to_out'] in kps_list:
      value = 'Yes'
      return value
    else:
      value ='No'
      return value

def GetKPList(spark, props, table):
    df=spark.sql('SHOW PARTITIONS '+props['hive_db_name_dest']+'.'+table) 
    df=df.select(substring('partition',15,26).alias('key_partitions_only_in_source'))
    kps_list = df.select('key_partitions_only_in_source').collect()
    print(kps_list)
    kps_list = [str(row.key_partitions_only_in_source) for row in kps_list]
    print(kps_list)      
    #kps_list = ', '.join(kps_list)
    return kps_list
      
def Get_MAX_KP_Below_Odate(partitions_list, props):
    #partitions_list = ["'"+kp+"'" for kp in partitions_list if kp < props['partition_to_out']]
    partitions_list = [kp for kp in partitions_list if kp < props['partition_to_out']]
    print(partitions_list)
    print(props['partition_to_out'])
    partitions_list = "'"+partitions_list[-1]+"'"
    return partitions_list



def Get_Max_KP_But_Below_Odate(spark, props, table):
    #df=spark.sql('SHOW PARTITIONS '+props['hive_db_name_dest']+'.'+'tbl_ta_notation') 
    df=spark.sql('SHOW PARTITIONS '+'m24_trr'+'.'+'tbl_ta_notation') 
    df=df.select(substring('partition',15,26).alias('key_partitions_only_in_source'))
    kps_list = df.select('key_partitions_only_in_source').collect()
    print(kps_list)
    #kps_list = [str(row.key_partitions_only_in_source) for row in kps_list if kp <= props['partition_to_out']]
    kps_list = [str(row.key_partitions_only_in_source) for row in kps_list]
    kps_list = [kp for kp in kps_list if kp < props['partition_to_out']]
    print(kps_list)      
    #max_kp_but_below_odate = max(kps_list)
    #max_kp_but_below_odate = ["'"+kp+"'" for kp in max_kp_but_below_odate]
    max_kp_but_below_odate = kps_list[-1]
    #max_kp_but_below_odate = "'"+max_kp_but_below_odate+"'"
    return max_kp_but_below_odate







def Read_table_definitions(table_def_file,table,props):
    """Function which reads the table definitions from a file"""
    print(table_def_file)
    a=os.getcwd()
    print(a)
    with io.open(table_def_file, encoding="utf-8") as f:
        params = f.readlines()
    table_def=[]
    for row in params:
        param = row.split(';')
        if param[0] == table:
            table_def='CREATE EXTERNAL TABLE IF NOT EXISTS '+props['hive_db_name_dest']+'.'+param[0]+' ('+param[1]+') PARTITIONED BY (key_partition string) LOCATION '+param[2]
            table_def_temp='CREATE EXTERNAL TABLE IF NOT EXISTS '+props['hive_db_name_dest']+'.'+param[0]+'_temp'+' ('+param[1]+') PARTITIONED BY (key_partition string) LOCATION '+param[2].replace('tbl_bref_triggers', 'tbl_bref_triggers_temp')
    return table_def, table_def_temp

def determine_partition_date(spark,props,table='be4_ndod.alertcredit_indicator'):
    """
    Function to determine the partition based on the odate
    """
    today=props['partition_to_out'] 
    print('TODAY: ',today)
    partitions=spark.sql("SELECT DISTINCT(CONCAT(replace(substr(alertcredit_indicator.date_extract,1,10),'-',''),'0000')) AS partition_key FROM "+table)
    #REGEXP_REPLACE('DOM_10GB_mth','[^0-9]+', "")
    #partitions=partitions.select(substring('partition',15,26).alias('partition_key'))
    # partitions=partitions.select(regex_replace('partition','[^0-9]+','').alias('partition_key'))
    partitions=partitions.withColumn('partition_key',partitions['partition_key'].cast(DoubleType()))
    # test_list=[[201912300000]]
    # test_df=spark.createDataFrame(test_list)
    # partitions = partitions.union(test_df)
    new_part = partitions.select('partition_key').filter('partition_key<='+today)
    # new_part=new_part.withColumn('partition_key',new_part['partition_key'].cast(DoubleType()))
    try:
        max_value = str(int(new_part.agg(SF.max(new_part.partition_key)).first()[0]))
        print('Partition to use: ',max_value)
        return max_value
    except TypeError:
        print("There are no partitions to load!")
        return ''
    
    #return '202012310000'

def determine_partition_date_alt(spark,props,table, primary_part,table_type):
    """
    Function to determine the partition based on the odate
    """
    today=props['partition_to_out']
    print('TODAY: ',today)
    partitions=spark.sql('SHOW PARTITIONS '+table)
    #REGEXP_REPLACE('DOM_10GB_mth','[^0-9]+', "")
    # partitions=partitions.select(substring('partition',15,26).alias('partition_key'))
    partitions=partitions.select(regexp_replace('partition','[^0-9]+','').alias('partition_key'))
    # partitions=partitions.withColumn('partition_key',regex_replace(col('partition'),'[^0-9]+','').cast(DoubleType()))
    # partitions=partitions.withColumn('partition_key',partitions['partition_key'].cast(DoubleType()))
    print('table_type: ',table_type)
    partition_to_test = partitions.select("partition_key").rdd.flatMap(lambda x: x).collect()
    #partition_to_test=partitions.partition_key
    print(partition_to_test,today[0:8])
    print(type(partition_to_test),type(today))
    today=datetime.datetime.strptime(today[0:8], '%Y%m%d')
    primary_part=datetime.datetime.strptime(primary_part[0:8], '%Y%m%d')
    partition_to_test=[datetime.datetime.strptime(test[0:8], '%Y%m%d') for test in partition_to_test]
    print(partition_to_test,today)
    print(type(partition_to_test),type(today))

    if table_type == 'P':
        print('entered table_type=P')
        if today in partition_to_test:
            print(today,' in partition_keys')
            return str(int(str(today)[0:10].replace('-',''))*10000), str(int(str(today)[0:10].replace('-',''))*10000)
        else:
            print(today,'not in partition_keys')
            today=today-datetime.timedelta(1)
            
            if today in partition_to_test:
                print(today,' in partition_keys')
                return str(int(str(today)[0:10].replace('-',''))*10000), str(int(str(today)[0:10].replace('-',''))*10000)
            else:
                print("Table does not have partition")
                print('BREAK')
                return 'BREAK',str(int(str(today)[0:10].replace('-',''))*10000)
    elif table_type=='S':
        print('entered table_type=S')
        if primary_part in partition_to_test:
            print(primary_part,' in partition_keys')
            return str(int(str(primary_part)[0:10].replace('-',''))*10000),str(int(str(primary_part)[0:10].replace('-',''))*10000)
        elif primary_part-datetime.timedelta(1) in partition_to_test:
            print(primary_part-1,' in partition_keys')
            return str(int(str(primary_part-datetime.timedelta(1))[0:10].replace('-',''))*10000),str(int(str(primary_part)[0:10].replace('-',''))*10000)
        else:
            print('Table '+table+' does not have good partitions present')
    elif table_type=='A':#asof
        print('entered table_type=A')  
        if primary_part in partition_to_test:
            print(primary_part,' in partition_keys')
            return str(int(str(primary_part)[0:10].replace('-',''))*10000),str(int(str(primary_part)[0:10].replace('-',''))*10000)
        else:
            map_partitions = sorted(i for i in partition_to_test if i<=primary_part)
            return str(int(str(map_partitions[-1])[0:10].replace('-',''))*10000),str(int(str(primary_part)[0:10].replace('-',''))*10000)  




def get_partition(tmpodat):
    return '20'+tmpodat+'0000'


def transform_into_friday(odate):
    odat_trans = datetime.datetime.strptime(odate,'%y%m%d')
    if odat_trans.weekday()<4:
        time_gap=odat_trans.weekday()%7+3
        odat_trans=odat_trans-datetime.timedelta(days = time_gap)
        return str(odat_trans).replace('-','')[2:8]
    else:
        return str(odat_trans).replace('-','')[2:8]  


def DeleteTemporaryTable(spark, delete_statement1, delete_statement2):
  if delete_statement1 == '' and delete_statement2 == '':
    print("No temporary table to delete")
  else:
    spark.sql(delete_statement1)
    spark.sql(delete_statement2)

def apply_activation_odat_j1(props , tmpODAT ):
    if props['activation_odat_j1']=='yes' :
        get_String_partition_Str = Transform_ODAT_from_CTM(str(tmpODAT))
        get_String_partition_Str  = Get_Previous_Day(get_String_partition_Str)
        props['ODAT'] = get_String_partition_Str
        return props
    elif props['activation_odat_j1']=='no':
        get_String_partition_Str = Transform_ODAT_from_CTM(str(tmpODAT))
        props['ODAT'] = get_String_partition_Str
        return props
        
def Transform_ODAT_from_CTM(dateCTM) :
    FinaldateCTM = "{0}{1}{2}".format('20',dateCTM , '0000' )
    return str(FinaldateCTM)
    
def Get_Previous_Day(maStringPartition):
    from datetime import datetime,timedelta
    import calendar
    datetime_object = datetime.strptime(maStringPartition, '%Y%m%d0000')
    if calendar.day_name[datetime_object.weekday()]=='Monday':
        MaNewDate = datetime_object  - timedelta(3)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return str(MaNewDate.strftime("%Y%m%d0000"))
    elif calendar.day_name[datetime_object.weekday()] in ['Tuesday','Wednesday','Thursday', 'Friday','Saturday']:
        MaNewDate = datetime_object  - timedelta(1)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return str(MaNewDate.strftime("%Y%m%d0000"))
    else:
        # WHEN SUNDAY - load friday
        MaNewDate = datetime_object  - timedelta(2)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return MaNewDate.strftime("%Y%m%d0000")
        print('********************** WARNING error : partition out of weekdays *************************')
        print('*******************************************************************************************')