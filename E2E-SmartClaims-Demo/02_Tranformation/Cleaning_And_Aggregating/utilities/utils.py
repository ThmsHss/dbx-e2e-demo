"""
The 'utilities' folder contains Python modules.
Keeping them separate provides a clear overview
of utilities you can reuse across your transformations.
"""
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import dlt
import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf
from typing import Iterator
import random

@udf(returnType=FloatType())
def distance_km(distance_miles):
    """
    Converts distance in miles to kilometers.

    Example usage:

    import dlt
    from pyspark.sql.functions import col
    from utilities import new_utils

    @dlt.table
    def my_table():
        return (
            spark.read.table("samples.nyctaxi.trips")
            .withColumn("trip_distance_km", new_utils.distance_km(col("trip_distance")))
        )
    """
    # 1 mile = 1.60934 km
    return distance_miles * 1.60934


# Function to get latitude and longitude from geocoding result
def geocode(geolocator, address):
    try:
      #Skip the API call for faster demo (remove this line for ream)
      return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
      location = geolocator.geocode(address)
      if location:
          return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
      print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      

@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))