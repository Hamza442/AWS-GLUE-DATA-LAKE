import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from delta.tables import *
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Source parameters
glue_database = "hamzatest"
glue_table = "survey_response_details"
additional_options = {"mergeSchema": "true"}

# table parameters
primary_key = "survey_details_id"
partition_cols_to_remove = ["partition_0","partition_1","partition_2"]
date_column = 'process_date'
full_load_max_date = '1900-01-01 00:00:00'
full_load_where_condtition = "where a.row_num=1 AND (A.op IS NULL OR A.op='I' OR A.op='U')"
incremental_load_where_condtition = "where a.row_num=1"
base_s3_path = "s3://test-bucket-ssi/test-delta-tables/" # destination table path
table_name = "survey_response_details_delta_new" # destination table name
final_base_path = "{base_s3_path}/deltalake/{table_name}".format(
    base_s3_path=base_s3_path, table_name=table_name
)
# athena
workgroup='cch_edw_v3'
athena_output_location = 's3://test-bucket-ssi/Hamza/athena/'

load_sql_template="""
    select * from (
    select  row_number() over (partition by {primary_key} order by {date_column} desc ) as row_num, * from incremental_table
    ) a
    {where}
    """
CREATE_EXTERNAL_TABLE = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table}
    LOCATION '{table_path}'
    TBLPROPERTIES (
    'table_type'='DELTA'
    );
    """

sql_to_remove_deletes = "select * from process_table where (op IS NULL OR op='I' OR op='U')"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
athena_client = boto3.client('athena')


def create_external_table(athena_client
                          ,query
                          ,glue_database
                          ,athena_output_location
                          ,workgroup
                          ,final_base_path
                          ,table_name):
                              
        response = athena_client.start_query_execution(
                        QueryString=query.format(table=table_name,table_path=final_base_path,),
                        QueryExecutionContext={ 'Database': glue_database },
                        ResultConfiguration={ 'OutputLocation': athena_output_location },
                        WorkGroup=workgroup)
        print("=====================ExternalTable query result",response)
    
    

def get_source_table(glue_database,glue_table,additional_options):
    df = glueContext.create_dynamic_frame.from_catalog(database=glue_database
                                                        ,table_name=glue_table
                                                        , additional_options=additional_options)
    return df.toDF()


def remove_columns(df,cols):
    return df.drop(*cols)


def get_destination_table(spark,final_base_path):
    return DeltaTable.forPath(spark, final_base_path)

    
def create_spark_temp_view(df,table_name):
    df.createOrReplaceTempView(f"{table_name}")


def check_table_exist(spark,final_base_path):
    try:
        df_delta = get_destination_table(spark,final_base_path)
    except Exception as e:
        df_delta = None
    return df_delta

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
              ,final_base_path):
                  
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
    incremental_df.write.format("delta").mode("overwrite").save(final_base_path)


def incremental_load(df
                    ,spark
                    ,final_base_path
                    ,date_column
                    ,primary_key
                    ,incremental_load_where_condtition
                    ,load_sql_template
                    ,sql_to_remove_deletes):
                        
    incremental_df = df
    print("========================Get Delta Table========================")
    df_delta = get_destination_table(spark,final_base_path)
    print("========================Get max_date==========================")
    spark_df = df_delta.toDF()
    max_date = spark_df.agg(max(date_column)).collect()[0][0]
    print("max_date=",max_date)
    incremental_df = incremental_df.where(col(date_column)>max_date)
    if not incremental_df.rdd.isEmpty():
        print("========================Creating Spark View========================")
        create_spark_temp_view(incremental_df,'incremental_table')
        print("========================Removing Duplicates========================")
        incremental_df = spark.sql(load_sql_template.format(primary_key=primary_key
                                                        ,date_column=date_column
                                                        ,where=incremental_load_where_condtition))
        print("=========================Executing Merge Statement=========================")
        df_delta.alias("full_df").merge(source = incremental_df.alias("append_df")
                                    ,condition = expr(f"append_df.{primary_key} = full_df.{primary_key}")).whenMatchedDelete().execute()
        create_spark_temp_view(incremental_df,'process_table')
        incremental_df = spark.sql(sql_to_remove_deletes)

        incremental_df = remove_columns(incremental_df,["row_num","Op"])
        print("=========================Inserting Into Final Table=========================")
        incremental_df.write.format("delta").mode("append").save(final_base_path)
    else:
        print("=========================No new data=========================")


def main(spark
        ,date_column
        ,full_load_max_date
        ,load_sql_template
        ,primary_key
        ,full_load_where_condtition
        ,incremental_load_where_condtition
        ,sql_to_remove_deletes
        ,final_base_path
        ,additional_options
        ,athena_client
        ,workgroup
        ,athena_output_location
        ,CREATE_EXTERNAL_TABLE):
    
    print("========================Getting Source Table========================")
    df = get_source_table(glue_database,glue_table,additional_options)
    partition_cols_to_remove = get_partition_cols(df)
    df = remove_columns(df,partition_cols_to_remove)
    print("========================Checking Source Table Exist========================")
    table_load_flage = check_table_exist(spark,final_base_path)
    print("========================Data Loading Flag = ",table_load_flage)
    
    if table_load_flage is None:
        print("========================Starting Full Load========================")
        print("========================Check Op Column===========================")
        op_col_flag = check_op_col(df)
        print("========================Op Column Exist = ",op_col_flag)
        if not op_col_flag:
            full_load_where_condtition=incremental_load_where_condtition
            
        full_load(df
              ,spark
              ,date_column
              ,full_load_max_date
              ,load_sql_template
              ,primary_key
              ,full_load_where_condtition
              ,final_base_path)
        print("========================Creating External Table========================")
        create_external_table(athena_client
                          ,CREATE_EXTERNAL_TABLE
                          ,glue_database
                          ,athena_output_location
                          ,workgroup
                          ,final_base_path
                          ,table_name)
    else:
        print("========================Starting Incremental Load========================")
        incremental_load(df
                    ,spark
                    ,final_base_path
                    ,date_column
                    ,primary_key
                    ,incremental_load_where_condtition
                    ,load_sql_template
                    ,sql_to_remove_deletes)
    
    
# Starting Flow Execution
main(spark
    ,date_column
    ,full_load_max_date
    ,load_sql_template
    ,primary_key
    ,full_load_where_condtition
    ,incremental_load_where_condtition
    ,sql_to_remove_deletes
    ,final_base_path
    ,additional_options
    ,athena_client
    ,workgroup
    ,athena_output_location
    ,CREATE_EXTERNAL_TABLE)

job.commit()