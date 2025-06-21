from pyspark.sql.functions import col, lit, struct

@dlt.table(
    name="telematics",
    comment="Parsed Kinesis data as map with metadata struct",
    table_properties={"quality": "bronze"}
)
def raw_telematics():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/dbdemos_smart_claims/smart_claims_schema/volume_claims/Telematics")
    )

    # Exclude unwanted columns
    df = df.drop("_rescued_data")

    # Cast all remaining columns to string
    string_df = df.select([col(c).cast("string").alias(c) for c in df.columns])

    result_df = string_df.withColumn(
            "stream_metadata",
            struct(
                lit("1EEOIV2PEVM3OXGOT").alias("partitionKey"),
                lit("telematics-stream-tmh").alias("stream"),
                lit("shardId-000000000000").alias("shardId"),
                lit("49664459073344232164086232308195477890692506428496674818").alias("sequenceNumber"),
                lit("2025-06-21T12:12:39.589Z").alias("approximateArrivalTimestamp")
            )
        )

    return result_df