import sys
import calendar
import pyspark.sql.functions as F
from pyspark.sql import *  
from pyspark.sql.functions import *
import datetime
import io
from Audit_trail import *

def ReadingGlobalParams():
    """
    This function read Global parameters given in 1st position of spark submit command , 
    it returns all param generated from XLDeploy into Host server
    """
    props = {}
    with open(sys.argv[1], "r") as f:
    #with open("/sluafrl97dwb1/appli/L97/L97_DLOAD_CONSO/etc/Globalparams.conf", "r") as f:
        for line in f:
            (key, val) = line.split("==")
            props[key] = str(val.rstrip()) 
    #read tables from param file
    print("Global Params Read from XLDeploy dictionary : ", props)
    props['history_audit_tabel_name_Final'] = props['hive_db_name_dest']+'.'+props['history_audit_tabel_name']
    props = clean_dico_XLD_From_quotes(props)
    return props

def clean_dico_XLD_From_quotes(props):
    "removes any quotes from the dictionary"
    for k,v in props.items():
        if props[k][-1]=='"' and props[k][0]=='"' :
            props[k] = props[k].replace('"', '')
    return props

def Add_partition_toExisting_Not_Pre_Partitioned_Table___To_Hive(spark , props , dicoTable, audit_schema, max_col_id ):
    import time
    print('table found exist - Partition NOT IN')
    _partition_Str = get_Partition_toLoad_depending_on_partition_type_D_M_or_P( spark, props )
    print('_partition_Str******************' , _partition_Str)
    dicoTable[props['TrueNameOfPartkey']] = _partition_Str
    df_final_Oracle             = _read_oracle_data_add_partition_to_table(             spark  , props , _partition_Str  )
    sampleQuery ="( select  count(*) as cnt from {0} )".format( props['Oracletabel'] )
    df                = read_oracle_data(       spark , props , sampleQuery )
    #cast df to extract count(*)
    #dicoTable['df'] = df_final_Oracle.count()
    dicoTable['df'] = int(df.select('cnt').collect()[0].cnt)
    newOodercolumns = compare_schema_oracle_hive_column_added_management(              spark  , props , dicoTable  , df_final_Oracle, audit_schema, max_col_id )
    print('newOodercolumns after create  ', newOodercolumns)
    print('df_final_Oracle  ', df_final_Oracle.columns)
    # return a list with whole columns order , adn refresh 
    if newOodercolumns: # that means that some columns were added
        df_hive = spark.sql("( SELECT * FROM {0} LIMIT 10 )".format(props['HiveTabel']))
        print('df_hive  ', df_hive.columns)
        df_final_Oracle = df_final_Oracle.select(df_hive.columns )
    # comparting cols
    df_final_Oracle = compare_schema_oracle_hive_column_deleted_management(             spark  , props , dicoTable  , df_final_Oracle, audit_schema, max_col_id )
    print('compare_schema_oracle_hive_column_deleted_management  ', df_final_Oracle.columns)
    drop_partition_if_exist_management(  spark  , props , dicoTable , _partition_Str, audit_schema, max_col_id )
    Write_SparkDF_Append_Existing_table_enrich_with_Partition(spark, df_final_Oracle ,  props  )
    count_spark_newrows = spark.sql("( SELECT count(*) as cnt FROM {0} WHERE {1}='{2}')".format(props['HiveTabel'],props['TrueNameOfPartkey'],dicoTable[props['TrueNameOfPartkey']]))
    number_of_spark_newrows = int(count_spark_newrows.select('cnt').collect()[0].cnt)
    Consistency = inspect_RDMBS_vs_HIVE_row_numbers( dicoTable['df'] , number_of_spark_newrows )
    max_col_id=write_audit_row_management( spark  , props , dicoTable  ,  Consistency, audit_schema, max_col_id  )
    max_col_id=write_audit_row_management( spark  , props , dicoTable  ,  'finish', audit_schema, max_col_id  )
    return max_col_id

def check_table_existance( spark , props   ):
    "Function that checks is a given table exists in the Hive DB"
    tbl_name = str(props['tab'])
    return spark.sql("show tables in  " + str(props['hive_db_name_dest']) ).filter(col('tableName') == 'tbl_'+tbl_name.lower() ).count() > 0




def get_last_day_previous_month(odate,delta_month):
    """
    Function finds the last day of a previous month, delta_month ago limit of 12 months
    """
    year='20'+odate[0:2]
    month=odate[2:4]
    month_int=int(month)
    last_month=month_int-delta_month%12
    if last_month<=0:
        last_month=12+last_month
        year=int(year)-1
    last_month=str(last_month).zfill(2)
    year = str(year)
    range_month=year+last_month+str(calendar.monthrange(int(year), int(last_month))[1])  
    return range_month

def get_last_work_day(last_day_previous_month):
    """
    Returns the last non-weekend day of a month
    """    
    odat_trans = datetime.datetime.strptime(last_day_previous_month[2::],'%y%m%d')
#    print(odat_trans.weekday())
    if odat_trans.weekday()>4:
        time_gap=odat_trans.weekday()%7-4
#        print(time_gap)
        odat_trans=odat_trans-datetime.timedelta(days = time_gap)
    else:
        odat_trans=odat_trans        
    return str(odat_trans).replace('-','')[0:8]

def select_str_for_insert(select_str,props):
    select_str=select_str.replace("[PRIMARY_TABLE_COLUMNS]",props["columns_str"]).replace("[HIVETABLE]",props["hivetable"]).replace("[YEARMONTH]",props["yearmonth"]).replace("[CODEINDICATORS]",props["codeindicators"]).replace("[SECONDLASTYEARMONTH]", props["secondlastyearmonth"]).replace("[PRIMARY_TABLE_COLUMNS_WITH_NO_OVERAGE]",props["columns_str_with_no_overage"]).replace("[PRIMARY_TABLE_COLUMNS_WITH_NO_OVERAGESTART]", props["columns_str_with_no_overagestart"])
    print("Select string: "+select_str)
    return select_str
    
def get_columns_as_str_deprecated(col_list,props):
    str_cols=''
    for element in col_list:
        str_cols=str_cols+' primary_table.'+str(element)+', '
    props["columns_str"]=str_cols
    return props


def Read_insert_parameters(select_str,props):
    """
    Function reads query parameters from file.
    """
    insert_str="INSERT OVERWRITE TABLE "+props["hivetable"]+" PARTITION (PARTITION_KEY=\'"+props["monthly_partition"]+"\') "+select_str
    return insert_str

def Read_table_and_code_indicators(props):
    """
    Gets the indicators, the tables and select str from the respective file
    """
    #copy props["monthly_indicators_file"]) from hdfs to etc
    tables=[]
    select_str=[]
    with io.open(props["monthly_indicators_file"], encoding="utf-8") as f:
        rows = f.readlines()
    for row in rows:
        row=row.split(";")
        list_str=''
        for ind in row[1].split(","):
            list_str=list_str+"\'"+ind+"\',"
        list_str=list_str[:-1]
        props[row[0]+"_ind"]=list_str
        tables.append(row[0])
        select_str.append(row[2])
    return props,tables,select_str


def get_columns_as_str( spark , props ):
    """
    this function transforms the columns in the hive table into a string
    """
    print('getting_table_cols')
    col_str = ''
    query = " ( SELECT * FROM {0} LIMIT 1 ) ".format(props['hivetable'])
    df_hive     = spark.sql(query)
    for col_1 in df_hive.columns[:-1]:
        print("primary_table."+str(col_1)+",")
        col_str=col_str+"primary_table."+str(col_1)+","

    print("### Columns for table ###")
    print(col_str[:-1])
    return col_str[:-1]


def Read_insert_parameters_alt(select_str,props):
    """
    Function reads query parameters from file.
    """
    drop_str="DROP TABLE IF EXISTS "+props['hivetable']+"_working"
    #insert_str="INSERT OVERWRITE TABLE "+props["hivetable"]+" PARTITION (PARTITION_KEY=\'"+props["monthly_partition"]+"\') "+select_str
    insert_str=" CREATE TABLE "+props["hivetable"]+"_working LOCATION 'hdfs:///dr/"+props["code_appli_dir"]+"/public/hive/"+props["hive_db"]+".db/"+props['OnlyTable']+"_working' AS "+select_str
    insert_str_2 = 'ALTER TABLE '+props["hivetable"]+'_working'+' SET TBLPROPERTIES ("external.table.purge"="true")'
    return insert_str, drop_str, insert_str_2


def load_working(spark,select_str,props, dicoTable  , audit_schema, max_col_id ):
    """
    Drops and reloads data into working table
    """
    select_str_tab=select_str_for_insert(select_str,props)
    insert_str, drop_str, insert_str_2=Read_insert_parameters_alt(select_str_tab,props)
    print("DROP WORKING")
    spark.sql(drop_str)
    max_col_id = write_audit_row_management(spark, props, dicoTable, 'drop_working_table', audit_schema, max_col_id)
    print("INSERT INTO WORKING")
    spark.sql(insert_str)
    print("External table purge true")
    spark.sql(insert_str_2)
    ##########COUNTING ROWS IN WORKING##############
    countQuery ="( select  count(*) as cnt from {0}_working )".format( props['HiveTabel'] )
    df = spark.sql(countQuery)
    dicoTable['df'] = int(df.select('cnt').collect()[0].cnt)
    max_col_id = write_audit_row_management(spark, props, dicoTable, 'insert_to_working', audit_schema, max_col_id)
    
    return max_col_id


def load_final_table(spark, props, dicoTable  , audit_schema, max_col_id ):
    """
    Inserts data from working into final table with the monthly partition.
    """
    print("INSERTING INTO FINAL TABLE")
    insert_str="INSERT OVERWRITE TABLE "+props["hivetable"]+" PARTITION (PARTITION_KEY=\'"+props["monthly_partition"]+"\') SELECT * FROM "+props["hivetable"]+"_working"
    spark.sql(insert_str)
    print("FINAL TABLE WAS LOADED WITH PARTITION: "+props["monthly_partition"])
    ##########COUNTS NUMBER OF INSERTED ROWS
    countQuery ="( select  count(*) as cnt from {0} WHERE PARTITION_KEY=\'{1}\')".format( props['HiveTabel'],props["monthly_partition"] )
    df1_cnt=dicoTable['df']
    df2 = spark.sql(countQuery)
    dicoTable['df'] = int(df2.select('cnt').collect()[0].cnt)
    max_col_id = write_audit_row_management(spark, props, dicoTable, 'insert_partition', audit_schema, max_col_id)
    ################CHECKS IF WORKING HAS SAME NUMBER OF ROWS OF NEW PARTITION
    Consistency = inspect_RDMBS_vs_HIVE_row_numbers(df1_cnt , dicoTable['df'])
    max_col_id=write_audit_row_management( spark  , props , dicoTable  ,  Consistency, audit_schema, max_col_id  )   
    return max_col_id