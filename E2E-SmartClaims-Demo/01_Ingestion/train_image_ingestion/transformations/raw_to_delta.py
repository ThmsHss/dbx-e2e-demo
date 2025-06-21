from pyspark.sql.functions import regexp_extract

@dlt.table(
  name="training_images",
  comment="Raw accident image training data ingested from S3",
  table_properties={"quality": "bronze"}
)
def raw_images():
  landing_catalog = spark.conf.get("landing_catalog")
  landing_schema = "landing"
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "BINARYFILE")
          .load(f"/Volumes/{landing_catalog}/{landing_schema}/training/images")
          .withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)\.png$", 2)))