"""
Unit Tests for Pyspark and Python functions
"""


import unittest
import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from my_functions import *

spark = SparkSession.builder.getOrCreate()


class TestStringMethods(unittest.TestCase):
  
  def test_func(self):
      for n in [("SOME BUSINESS NAME", "ANOTHER BUSINESS NAME", 84),
                ("some business name", "ANOTHER BUSINESS NAME", 84),
                ("some business name", "Name SOME business", 100),
                ("", "another business", 0)]:
        
          with self.subTest(n=n):
            result = simple_python_func(n[0], n[1])
            self.assertEqual(result, n[2])

  def test_upper(self):
      self.assertEqual('foo'.upper(), 'FOO')
      

class TestSparkMethods(unittest.TestCase):
  
  def test_spark(self):
    
    input_schema = StructType([StructField('account',      IntegerType(), True),
                               StructField('company_name', StringType(),  True),
                               StructField('account_type', StringType(),  True),
                               StructField('balance',      StringType(),  True)])

    input_data = [(100, 'company_a', 'a', 1), (200, 'company_b', 'b', 2), (300, 'company_c', 'c', 3)]

    # Create a Spark Dataframe
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)


    # Output test dataset
    output_schema = StructType([StructField('company_name', StringType(),  True),
                                StructField('sum',          FloatType(),  True)])

    output_data = [('company_a', 1.0), ('company_b', 2.0), ('company_c', 3.0)]

    output_df = spark.createDataFrame(data=output_data, schema=output_schema)

    # Generate the output of the function to test
    test_df = simple_transformation(input_df, 'company_name', 'balance')

    # Compare the functions output to the output DataFrame
    differences = test_df.subtract(output_df).count()
    
    self.assertEqual(differences, 0)