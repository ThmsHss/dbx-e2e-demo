from pyspark.sql import functions as F

@dlt.table(
    name="telematics",
    comment="Cleaned, preaggregated average telematics records",
    table_properties={"quality": "silver"}
)
def telematics():
  return (dlt.read('e2e_demo_claims.bronze.telematics').groupBy("chassis_no").agg(
                F.avg("speed").alias("telematics_speed"),
                F.avg("latitude").alias("telematics_latitude"),
                F.avg("longitude").alias("telematics_longitude")))
                