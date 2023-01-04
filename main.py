import pyspark

builder = pyspark.sql.SparkSession.builder \
    .master("local") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")

spark = builder.getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

df = spark.createDataFrame(data).toDF(*columns)

df.write \
  .format("delta") \
  .partitionBy("language") \
  .save("s3a://test-delta-lake-audits/test-reproduction-glue-partitions")

df.show()
