##import required modules
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from awsglue.utils import getResolvedOptions
import json
import boto3
import sys

args = getResolvedOptions(sys.argv, [
        'secret'
        ])

secret = args['secret']


ssm = boto3.client('ssm')
credentials = json.loads(ssm.get_parameter(Name=secret,WithDecryption=True)['Parameter']['Value'])
sql_con = credentials["sql_con"]
sql_user = credentials["sql_user"]
sql_password = credentials["sql_password"]
dim_s3_location = credentials["s3_destination_bucket"]
maskdata = credentials["maskdata"]

 
#Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")
 
#Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

    
DimProspectDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=Prospect;") \
    .option("dbtable", "(SELECT IndividualFirstName, IndividualLastName FROM model.DimProspect) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    
    
#if pii then proceed to mask fields, else, skip and write to S3 to S3
var = maskdata
if var == "true":
    
    DimProspectDF = DimProspectDF.withColumn("IndividualFirstName", lit("masked@masked.com")) \
        .withColumn("IndividualLastName", lit("masked@masked.com")) \
        .withColumn("JobName",lit("glue-job-prospectdb-data-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Prospect.DimProspect"))\
    
    #DimProspectDF.show()

    DimProspectDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/Prospect/DimProspect',mode='overwrite')
else:
    
    DimProspectDF = DimProspectDF.withColumn("JobName",lit("glue-job-prospectdb-data-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Prospect.DimProspect"))\
        
    DimProspectDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/Prospect/DimProspect',mode='overwrite')

DimProspectDemDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=Prospect;") \
    .option("dbtable", "(SELECT IndividualAge, IndividualGender, Individualethnicityidentificationsystemcode, IndividualEthnicityIdentificationSystemV2Code, IndividualAIQ_ID, IndividualInfutorPID FROM model.DimProspectDemographic) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

#if pii then proceed to mask fields, else, skip and write to S3
var = maskdata
if var == "true":
    
    DimProspectDemDF = DimProspectDemDF.withColumn("IndividualAge", lit("-1")) \
        .withColumn("IndividualGender", lit("masked")) \
        .withColumn("JobName",lit("glue-job-prospectdb-data-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Prospect.DimProspectDemographic"))\

    #DimProspectDemDF.show()

    DimProspectDemDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/Prospect/DimProspectDemographic',mode='overwrite')

else:
    
    DimProspectDemDF = DimProspectDemDF.withColumn("JobName",lit("glue-job-prospectdb-data-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Prospect.DimProspectDemographic"))\
    
    DimProspectDemDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/Prospect/DimProspectDemographic',mode='overwrite')

