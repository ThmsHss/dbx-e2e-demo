from pyspark.sql import types as T
"""
The 'utilities' folder contains Python modules.
Keeping them separate provides a clear overview
of utilities you can reuse across your transformations.
"""

def flatten_struct(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            for child in field.dataType:
                df = df.withColumn(field.name + '_' + child.name, F.col(field.name + '.' + child.name))
            df = df.drop(field.name)
    return df