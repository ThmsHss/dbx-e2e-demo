
# --- CLAIM-POLICY ---
@dlt.table(comment = "Curated claim joined with policy records")
def claim_policy():
    # Read the staged policy records
    policy = dlt.read("policy")
    # Read the staged claim records
    claim = dlt.readStream("claim")
    
    return claim.join(policy, on="policy_no")



# --- CLAIM-POLICY-TELEMATICS ---
@dlt.table(comment="claims with geolocation latitude/longitude")
def claim_policy_telematics():
  t = dlt.read("telematics")
  claim = dlt.read("claim_policy").where("address is not null")
  return (claim.withColumn("lat_long", get_lat_long(col("address")))
               .join(t, on="chassis_no"))