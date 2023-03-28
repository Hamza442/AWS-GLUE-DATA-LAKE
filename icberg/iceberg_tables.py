import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import time
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Source parameters
glue_database = "hamzatest"
glue_table = "survey_response_details"
additional_options = {"mergeSchema": "true"}

CATALOG='glue_catalog'
DATABASE='hamzatest'
TABLE='survey_response_details_iceberg_new'
primary_key = "survey_details_id"
#source_path = "s3://test-bucket-ssi/harris/candidate/"
#source_table_connection = 's3'
#source_table_format = 'parquet'
partition_cols_to_remove = ["partition_0","partition_1","partition_2"]
date_column = 'process_date'
full_load_max_date = '1900-01-01 00:00:00'
full_load_where_condtition = "where a.row_num=1 AND (A.op IS NULL OR A.op='I' OR A.op='U')"
incremental_load_where_condtition = "where a.row_num=1"
iceberg_format_version = '2'

load_sql_template="""
    select * from (
    select  row_number() over (partition by {primary_key} order by {date_column} desc ) as row_num, * from incremental_table
    ) a
    {where}
    """
merge_sql_stmnt ="""
    MERGE INTO {CATALOG}.{DATABASE}.{TABLE} AS data
    USING input_data_updates AS updates
    ON data.{primary_key} = updates.{primary_key}
    WHEN MATCHED THEN DELETE 
    """
sql_to_remove_deletes = "select * from process_table where (op IS NULL OR op='I' OR op='U')"


def get_source_table(glue_database,glue_table,additional_options):
    df = glueContext.create_dynamic_frame.from_catalog(database=glue_database
                                                        ,table_name=glue_table
                                                        , additional_options=additional_options)
    return df.toDF()


def remove_columns(df,cols):
    return df.drop(*cols)


def get_destination_table(spark,CATALOG,DATABASE,TABLE):
    return spark.table(f'{CATALOG}.{DATABASE}.{TABLE}')

    
def create_spark_temp_view(df,table_name):
    df.createOrReplaceTempView(f"{table_name}")


def check_table_exist(spark,CATALOG,DATABASE,TABLE):
    try:
        df_iceberg = get_destination_table(spark,CATALOG,DATABASE,TABLE)
    except Exception as e:
        df_iceberg = None
    return df_iceberg

def check_op_col(df):
    columns_list = df.columns
    return [True for col in columns_list if col.lower()=="op" ]

def get_partition_cols(df):
    my_list = df.columns
    substring = 'partition_'
    return [element for element in my_list if substring in element]


def full_load(df
              ,spark
              ,date_column
              ,full_load_max_date
              ,load_sql_template
              ,primary_key
              ,full_load_where_condtition
              ,CATALOG
              ,DATABASE
              ,TABLE
              ,iceberg_format_version):
                  
    incremental_df = df
    incremental_df = incremental_df.where(col(date_column)>full_load_max_date)
    print("========================Creating Spark View========================")
    create_spark_temp_view(incremental_df,'incremental_table')
    print("========================Removing Duplicates========================")
    incremental_df = spark.sql(load_sql_template.format(primary_key=primary_key
                                                        ,date_column=date_column
                                                        ,where=full_load_where_condtition))
    incremental_df = remove_columns(incremental_df,["row_num","Op"])
    print("=========================Inserting Into Final Table=========================")
    incremental_df.writeTo(f'{CATALOG}.{DATABASE}.{TABLE}').tableProperty('format-version', iceberg_format_version).create()
 
  
def incremental_load(df
                    ,spark
                    ,CATALOG
                    ,DATABASE
                    ,TABLE
                    ,date_column
                    ,primary_key
                    ,incremental_load_where_condtition
                    ,load_sql_template
                    ,merge_sql_stmnt
                    ,sql_to_remove_deletes):
                        
    incremental_df = df
    print("========================Get Iceberg Table========================")
    df_iceberg = get_destination_table(spark,CATALOG,DATABASE,TABLE)
    print("========================Get max_date========================")
    max_date = df_iceberg.agg(max(date_column)).collect()[0][0]
    print("max_date=",max_date)
    incremental_df = incremental_df.where(col(date_column)>max_date)
    if not incremental_df.rdd.isEmpty():
        print("========================Creating Spark View========================")    
        create_spark_temp_view(incremental_df,'incremental_table')
        print("========================Removing Duplicates========================")
        incremental_df = spark.sql(load_sql_template.format(primary_key=primary_key
                                                        ,date_column=date_column
                                                        ,where=incremental_load_where_condtition))
        create_spark_temp_view(incremental_df,'input_data_updates')
        print("=========================Executing Merge Statement=========================")
        spark.sql(merge_sql_stmnt.format(CATALOG=CATALOG
                                     ,DATABASE=DATABASE
                                     ,TABLE=TABLE
                                     ,primary_key=primary_key))
        create_spark_temp_view(incremental_df,'process_table')
        incremental_df = spark.sql(sql_to_remove_deletes)
        incremental_df = remove_columns(incremental_df,["row_num","Op"])
        print("=========================Inserting Into Final Table=========================")
        incremental_df.writeTo(f'{CATALOG}.{DATABASE}.{TABLE}').append()
    else:
        print("=========================No new data=========================")
    


def main(spark
        ,date_column
        ,full_load_max_date
        ,load_sql_template
        ,primary_key
        ,full_load_where_condtition
        ,CATALOG
        ,DATABASE
        ,TABLE
        ,iceberg_format_version
        ,incremental_load_where_condtition
        ,merge_sql_stmnt
        ,sql_to_remove_deletes
        ,glue_database
        ,glue_table
        ,additional_options):
    
    print("========================Getting Source Table========================")
    df = get_source_table(glue_database,glue_table,additional_options)
    partition_cols_to_remove = get_partition_cols(df)
    df = remove_columns(df,partition_cols_to_remove)
    print("========================Checking Source Table Exist========================")
    table_load_flage = check_table_exist(spark,CATALOG,DATABASE,TABLE)
    
    if table_load_flage is None:
        print("========================Starting Full Load========================")
        print("========================Check Op Column===========================")
        op_col_flag = check_op_col(df)
        print("========================Op Column Exist = ",op_col_flag)
        if not op_col_flag:
            full_load_where_condtition=incremental_load_where_condtition
        print("========================Starting Full Load========================")
        full_load(df
              ,spark
              ,date_column
              ,full_load_max_date
              ,load_sql_template
              ,primary_key
              ,full_load_where_condtition
              ,CATALOG
              ,DATABASE
              ,TABLE
              ,iceberg_format_version)
    else:
        print("========================Starting Incremental Load========================")
        incremental_load(df
                    ,spark
                    ,CATALOG
                    ,DATABASE
                    ,TABLE
                    ,date_column
                    ,primary_key
                    ,incremental_load_where_condtition
                    ,load_sql_template
                    ,merge_sql_stmnt
                    ,sql_to_remove_deletes)
    
    
# Starting Flow Execution
main(spark
    ,date_column
    ,full_load_max_date
    ,load_sql_template
    ,primary_key
    ,full_load_where_condtition
    ,CATALOG,DATABASE
    ,TABLE
    ,iceberg_format_version
    ,incremental_load_where_condtition
    ,merge_sql_stmnt
    ,sql_to_remove_deletes
    ,glue_database
    ,glue_table
    ,additional_options)
job.commit()
