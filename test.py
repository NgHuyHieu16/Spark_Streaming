from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.session import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder\
                    .master('local[1]')\
                    .appName('bai_thuc_hanh_3.com')\
                    .getOrCreate()
                    
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

schema = StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StructType(), True)
])

df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()

ls = df.collect()
ls.append({'firstname':'John', 'middlename':'J', 'lastname':'Doe'})
df = spark.createDataFrame(ls)
df.show()