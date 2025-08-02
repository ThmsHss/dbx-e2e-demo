@dlt.table(comment = "Curated claim joined with policy records")
def claim_policy():
    # Read the staged policy records
    policy = dlt.read("policy")
    # Read the staged claim records
    claim = dlt.readStream("claim")
    
    return claim.join(policy, on="policy_no")