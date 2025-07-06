from utilities import utils
from pyspark.sql import functions as F

@dlt.table(
    name="claim",
    comment="Cleaned claim records",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({"valid_claim_number": "claim_no IS NOT NULL"})
def claim():
    # Read the staged claim records into memory
    claim = dlt.readStream("e2e_demo_claims.bronze.claim")
    claim = utils.flatten_struct(claim)  
    
    # Update the format of all date/time features
    return (claim.withColumn("claim_date", F.to_date(F.col("claim_date")))
                 .withColumn("incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd"))
                 .drop('_rescued_data'))