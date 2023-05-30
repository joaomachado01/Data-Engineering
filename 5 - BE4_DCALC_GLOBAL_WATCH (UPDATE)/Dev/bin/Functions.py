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
from datetime import timedelta

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


def Read_insert_parameters(insert_param_file,table,props,spark):
    """
    Function reads query parameters from file.
    """
    with io.open(insert_param_file, encoding="utf-8") as f:
        params = f.readlines()
    insert = []
    join_tables = []
    type_table = []
    for row in params:
        param = row.split(';')
        if param[0]==table:

            # Get max_kp_below_odate for table
            max_kp_below_odate = GetKPList(spark, props, table)
            max_kp_below_odate = Get_MAX_KP_Below_Odate(max_kp_below_odate, props)
            print("################### Final key_partition to load ###############")
            print(max_kp_below_odate)

            # Get last kp from l97 and m62 tables
            be4 = props['hive_db_name_dest']

            if len(param[4].split(',')) > 1:
                # L97
                props['hive_db_name_dest'] = param[4].split(',')[0].split('.')[0] # l97 schema
                previous_kp_l97 = GetKPList(spark, props, param[4].split(',')[0].split('.')[1]) # l97 table
                previous_kp_l97 = Get_MAX_KP_Below_Odate(previous_kp_l97, props)
                print("################### key_partition of l97 ###################")
                print(previous_kp_l97)

                # M62
                props['hive_db_name_dest'] = param[4].split(',')[1].split('.')[0] # m62 schema
                previous_kp_m62 = GetKPList(spark, props, param[4].split(',')[1].split('.')[1])  # m62 table
                previous_kp_m62 = Get_MAX_KP_Below_Odate(previous_kp_m62, props)
                print("################### key_partition of m62 ###################")
                print(previous_kp_m62)

                # tethys table - tiers
                props['hive_db_name_dest'] = param[4].split(',')[2].split('.')[0] # tethys table schema
                asofdatetiers = GetKPList(spark, props, param[4].split(',')[2].split('.')[1])  # tethys table
                asofdatetiers = Get_MAX_KP_Below_Odate(asofdatetiers, props)
                print("################### key_partition of tiers ###################")
                print(asofdatetiers)

                # tethys table - modele
                props['hive_db_name_dest'] = param[4].split(',')[3].split('.')[0] # tethys table schema
                asofdatemodele = GetKPList(spark, props, param[4].split(',')[3].split('.')[1])  # tethys table
                asofdatemodele = Get_MAX_KP_Below_Odate(asofdatemodele, props)
                print("################### key_partition of modele ###################")
                print(asofdatemodele)



                props['hive_db_name_dest'] = be4

            else:
                print("Global fiche table processing")

                # L97
                previous_kp_l97 = ''
                print("################### key_partition of l97  ###################")
                print(previous_kp_l97)

                # M62
                previous_kp_m62 = ''
                print("################### key_partition of m62 ###################")
                print(previous_kp_m62)

                # tethys table - tiers
                asofdatetiers = ''
                print("################### key_partition of tiers ###################")
                print(asofdatetiers)

                # tethys table - modele
                asofdatemodele = ''
                print("################### key_partition of modele ###################")
                print(asofdatemodele)

                props['hive_db_name_dest'] = be4

            insert='INSERT OVERWRITE TABLE '+props['hive_db_name_dest']+'.'+param[0]+' PARTITION ('+props['TrueNameOfPartkey']+'=\''+props['partition_to_load']+'\') '+param[1].replace('[max_kp_below_odate]', max_kp_below_odate).replace('[previous_kp_l97]', previous_kp_l97).replace('[asofdatetiers]', asofdatetiers).replace('[asofdatemodele]', asofdatemodele).replace('[previous_kp_m62]', previous_kp_m62) 
            print("#################################### INSERT STATEMENT ####################################")
            print(insert)
            join_tables = param[2].split(',')
            type_table = param[3].split(',')
            # insert='SELECT '+param[1]+' FROM '+param[2]+' WHERE '+param[3]#+' PARTITION_KEY='+props['partition_str']
    return insert,join_tables,type_table
    # return insert

def Hive_Target_Table_Empty(spark, table, props):
    df=spark.sql('SELECT COUNT(*) AS rows FROM '+props['hive_db_name_dest']+'.'+table)
    rows = int(df.select('rows').collect()[0].rows)
    if rows == 0:
      value = 'Yes'
    else:
      value = 'No'
    return value


def GetKPList(spark, props, table):
    df=spark.sql('SHOW PARTITIONS '+props['hive_db_name_dest']+'.'+table)
    #df=df.select(substring('partition',15,26).alias('key_partitions_only_in_source'))
    if len(df.filter(col("partition").contains("-")).collect()) > 0:
        print("## ASOFDATE ###")
        print(table)
        df=df.select(substring('partition',20,10).alias('key_partitions_only_in_source'))
        df.show()
        kps_list = df.select('key_partitions_only_in_source').collect()
        print(kps_list)
        kps_list = [str(row.key_partitions_only_in_source) for row in kps_list]
        print(kps_list) 
    else:
        print("## KP ###")
        print(table)
        df=df.select(substring('partition',15,26).alias('key_partitions_only_in_source'))
        df.show()
        kps_list = df.select('key_partitions_only_in_source').collect()
        print(kps_list)
        kps_list = [str(row.key_partitions_only_in_source) for row in kps_list]
        print(kps_list)      
    #kps_list = ', '.join(kps_list)
    return kps_list


def Get_MAX_KP_Below_Odate(partitions_list, props):
    var_list = "-"
    if len([x for x in partitions_list if x.find(var_list) >= 0]) > 0:
        print("### The partitions list are in the format 2023-01-01 ###")

        #partitions_list = ["'"+kp+"'" for kp in partitions_list if kp < props['partition_to_out']]
        partitions_list = [kp for kp in partitions_list if kp.replace('-', '') < props['partition_to_out']]
        print(partitions_list)
        print(props['partition_to_out'])
        if len(partitions_list) > 0:
            # When multiple values were returned
            partitions_list = "'"+partitions_list[-1]+"'"
        else:
            partitions_list = "'"+''.join(partitions_list)+"'"
    else:
        print("### The partitions list are in the format 202301010000 ###")

        #partitions_list = ["'"+kp+"'" for kp in partitions_list if kp < props['partition_to_out']]
        partitions_list = [kp for kp in partitions_list if kp < props['partition_to_out']]
        print(partitions_list)
        print(props['partition_to_out'])
        if len(partitions_list) > 0:
            # When multiple values were returned
            partitions_list = "'"+partitions_list[-1]+"'"
        else:
            partitions_list = "'"+''.join(partitions_list)+"'"
    return partitions_list



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
            table_def='CREATE EXTERNAL TABLE IF NOT EXISTS '+props['hive_db_name_dest']+'.'+param[0]+' ('+param[1]+') PARTITIONED BY ('+props['TrueNameOfPartkey']+' string) LOCATION '+param[2]
    return table_def

def determine_partition_date(spark,props,table='m62_great.tbl_drp_contrats_weeklycrisis'):
    """
    Function to determine the partition based on the odate
    """
    today=props['partition_to_out']
    print('TODAY: ',today)
    partitions=spark.sql('SHOW PARTITIONS '+table)
    #REGEXP_REPLACE('DOM_10GB_mth','[^0-9]+', "")
    partitions=partitions.select(substring('partition',15,26).alias('partition_key'))
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
    partitions=partitions.select(regexp_replace('partition','[^0-9]+','').alias('partition_key'))
    print('table_type: ',table_type)
    partition_to_test = partitions.select("partition_key").rdd.flatMap(lambda x: x).collect()
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
        print("primary_part")
        print(primary_part)
        print("partition_to_test")
        print(partition_to_test)
        if primary_part in partition_to_test:
            print(primary_part,' in partition_keys')
            return str(int(str(primary_part)[0:10].replace('-',''))*10000),str(int(str(primary_part)[0:10].replace('-',''))*10000)
        else:
            print('Table '+table+' does not have good partitions present')
            return 'BREAK'
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
    
    
    
def apply_activation_odat_j1(props , tmpODAT ):
    from datetime import datetime
    if props['activation_odat_j1']=='yes' :
        get_String_partition_Str = get_partition(str(tmpODAT))
        get_String_partition_Str  = Get_Previous_Day(get_String_partition_Str)
        props['ODAT'] = get_String_partition_Str
        return props
    elif props['activation_odat_j1']=='no':
        #changed because of the schedulling
        get_String_partition_Str = Transform_ODAT_from_CTM(str(tmpODAT))
        datetime_object = datetime.strptime(get_String_partition_Str, '%Y%m%d0000')
        if calendar.day_name[datetime_object.weekday()] in ['Saturday']:
            MaNewDate = datetime_object  - timedelta(1)
            props['ODAT'] = MaNewDate.strftime("%Y%m%d0000")
        if calendar.day_name[datetime_object.weekday()] in ['Sunday']:
            MaNewDate = datetime_object  - timedelta(2)
            props['ODAT'] = MaNewDate.strftime("%Y%m%d0000")
        else:
            get_String_partition_Str = Transform_ODAT_from_CTM(str(tmpODAT))
            props['ODAT'] = get_String_partition_Str
        return props
    
    
def Transform_ODAT_from_CTM(dateCTM) :
    FinaldateCTM = "{0}{1}{2}".format('20',dateCTM , '0000' )
    return str(FinaldateCTM)


def Get_Previous_Day(maStringPartition):
    #cast string date to datetime object for calcul and cast back to string at the end of treatment
    from datetime import datetime
    datetime_object = datetime.strptime(maStringPartition, '%Y%m%d0000')
    if calendar.day_name[datetime_object.weekday()]=='Monday':
        # WHEN MONDAY
        MaNewDate = datetime_object  - timedelta(3)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return MaNewDate.strftime("%Y%m%d0000")
    elif calendar.day_name[datetime_object.weekday()] in ['Tuesday','Wednesday','Thursday', 'Friday', 'Saturday']:
        # WHEN OTHER WORKING DAYS OF THE WEEKEND
        MaNewDate = datetime_object  - timedelta(1)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return MaNewDate.strftime("%Y%m%d0000")
    else:
        # WHEN SUNDAY
        MaNewDate = datetime_object  - timedelta(2)
        print(maStringPartition ,' :' ,calendar.day_name[datetime_object.weekday()] , ' --- newDate : ' , MaNewDate.strftime("%Y%m%d0000") )
        return MaNewDate.strftime("%Y%m%d0000")