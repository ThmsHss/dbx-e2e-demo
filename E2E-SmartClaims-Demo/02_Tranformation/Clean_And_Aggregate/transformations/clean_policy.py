from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, col
from pyspark.sql import types as T

@dlt.table(
    name="policy",
    comment="Cleaned policy records",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({"valid_policy_number": "policy_no IS NOT NULL"})
def policy():
    # Read the staged policy records into memory
    return (dlt.readStream("e2e_demo_claims.bronze.policy")
                .withColumn("premium", F.abs(col("premium")))
                # Reformat the incident date values
                .withColumn("pol_eff_date", F.to_date(col("pol_eff_date"), "dd-MM-yyyy"))
                .withColumn("pol_expiry_date", F.to_date(col("pol_expiry_date"), "dd-MM-yyyy"))
                .withColumn("pol_issue_date", F.to_date(col("pol_issue_date"), "dd-MM-yyyy"))
                .drop('_rescued_data'))