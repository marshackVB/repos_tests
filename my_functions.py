from fuzzywuzzy import fuzz
import pyspark.sql.functions as func
from pyspark.sql.functions import col


def simple_python_func(string_col_1, string_col_2):
    """
    A Python function that could be used to create a Spark UDF.
    """

    similarity = fuzz.token_set_ratio(string_col_1, string_col_2)

    return similarity
  
  
def simple_transformation(df, groupby_col, sum_col):
    """
    A function that manipulates a Spark Dataframe and returns a new DataFrame
    """
    return df.groupBy(groupby_col).agg(func.sum(sum_col).alias('sum'))
  