#import required modules
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

#Load DimSource
#read table data into a spark dataframe
DimSourceDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "Model.DimSource") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

#Add metadata columns
DimSourceDF = DimSourceDF.withColumn("JobName",lit("glue-job-datawarehouse-dim-migration")) \
        .drop("maxrowversion") \
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("DataWarehouse.DimSource"))\
   
#DimSourceDF.show()

#write dataframe to S3 with overwrite option
DimSourceDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimSource',mode='overwrite')

#Load DimEmailAddress
#read table data into a spark dataframe
DimEmailAddressDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "Model.DimEmailAddress") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
   
#if pii then mask fields, else, skip and write to S3
var = maskdata

if var == "true":
   
    DimEmailAddressDF = DimEmailAddressDF.withColumn("EmailAddressNK", lit("masked")) \
        .withColumn("ProspectID", lit("masked")) \
        .withColumn("CEREmailID", lit("masked")) \
        .withColumn("JobName",lit("datawarehouse-dim-migration")) \
        .withColumn("JobRunDate",F.lit(datetime.datetime.today())) \
        .withColumn("TableName", lit("Model.DimEmailAddress")) \

    DimEmailAddressDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimEmailAddress',mode='overwrite')
else:
   
    DimEmailAddressDF = DimEmailAddressDF.withColumn("JobName",lit("datawarehouse-dim-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Model.DimEmailAddress"))\
       
    DimEmailAddressDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimEmailAddress',mode='overwrite')


#Load DimConstituentAggDemographic
#read table data into a spark dataframe
DimConstituentAggDemographicDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "(SELECT Age,ChildrenInterestsIndicator,Education,EthnicCode,EthnicGroup,EthnicIq,EthnicIqZip4,GrandChildrenIndicator,HomeToIncomeRatio,HomeToIncomeRatioZip4,IncomeChangeIn12Month,IncomeIqPlus,IncomeIqPlusDisplay,IncomeIqPlusDisplayZip4,IncomeIqPlusRatio,IncomeIqPlusRatioZip4,IncomeIqPlusZip4,LanguageCode,Marital,SpendexEducation,SpendexEducationZip4 FROM Model.DimConstituentAggDemographic) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    
#if pii then mask fields, else, skip and write to S3
var = maskdata

if var == "true":
    
    DimConstituentAggDemographicDF = DimConstituentAggDemographicDF.withColumn("Education", lit("masked")) \
        .withColumn("EthnicCode", lit("masked")) \
        .withColumn("EthnicGroup", lit("masked")) \
        .withColumn("EthnicIqZip4", lit("masked")) \
        .withColumn("GrandChildrenIndicator", lit("masked")) \
        .withColumn("LanguageCode", lit("masked")) \
        .withColumn("Marital", lit("masked")) \
        .withColumn("SpendexEducation", lit("masked")) \
        .withColumn("JobName",lit("datawarehouse-dim-migration")) \
        .withColumn("JobRunDate",F.lit(datetime.datetime.today())) \
        .withColumn("TableName", lit("Model.DimConstituentAggDemographic")) \

    DimConstituentAggDemographicDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimConstituentAggDemographic',mode='overwrite')
else:
    
    DimConstituentAggDemographicDF = DimConstituentAggDemographicDF.withColumn("JobName",lit("datawarehouse-dim-migration"))\
        .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
        .withColumn("TableName", lit("Model.DimConstituentAggDemographic"))\
        
    DimConstituentAggDemographicDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimConstituentAggDemographic',mode='overwrite')
    
#Load DimAddress
#read table data into a spark dataframe
DimAddressDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "(SELECT AddressSK, CityName, PostalCode, PostalCodeExtension, CountryCode, CountryDescription FROM Model.DimAddress) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    

DimAddressDF = DimAddressDF.withColumn("JobName",lit("datawarehouse-dim-migration"))\
    .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
    .withColumn("TableName", lit("Model.DimAddress"))\
    
DimAddressDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimAddress',mode='overwrite')

#Load FactConstituentAddress
#read table data into a spark dataframe
FactConstituentAddressDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "(SELECT ConstituentSK, AddressSK, ConstituentAddressPreferredIndicator FROM Model.FactConstituentAddress) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    

FactConstituentAddressDF = FactConstituentAddressDF.withColumn("JobName",lit("datawarehouse-dim-migration"))\
    .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
    .withColumn("TableName", lit("Model.FactConstituentAddress"))\
    
FactConstituentAddressDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/FactConstituentAddress',mode='overwrite')

#Load DimConstituent
#read table data into a spark dataframe
DimConstituent = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_con}:1433;databaseName=DataWarehouse;") \
    .option("dbtable", "(SELECT ConstituentSK, C360ConstituentNK FROM DataWarehouse.Model.DimConstituent) a") \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    
#DimConstituentDF.show()

DimConstituentDF = DimConstituent.withColumn("JobName",lit("glue-job-dimconstituent-data-migration"))\
    .withColumn("JobRunDate",F.lit(datetime.datetime.today()))\
    .withColumn("TableName", lit("Model.DimConstituent"))\

DimConstituentDF.write.option("header", "true").parquet(f's3://{dim_s3_location}/DATADB/DimConstituent',mode='overwrite')

