from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/cde_examples.ini')
s3BucketName = config['CDE-examples']['s3BucketName'].replace('"','').replace("\'",'')


## Launching Spark Session

spark = SparkSession\
    .builder\
    .appName("DataExploration")\
    .config("spark.yarn.access.hadoopFileSystems", s3BucketName)\
    .getOrCreate()
#    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\

## Creating Spark Dataframe from raw CSV datagov

df = spark.read.option('inferschema','true').csv(
  s3BucketName + "/LoanStats_2015_subset.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

## Showing Different Loan Status Values
df.select("loan_status").distinct().show()

## Types of Loan Status Aggregated by Count

print(df.groupBy('loan_status').count().show())
