# Databricks notebook source
# MAGIC %md ## Apply tests
# MAGIC If running interactively, see the list of pypi dependencies in job_runner.py

# COMMAND ----------

import unittest
import xml.etree.ElementTree as ET
import pandas as pd
import xmlrunner
import uuid
import io
import datetime
import json
import sys
import os

import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *

# COMMAND ----------

current_notebook_path = os.path.dirname(os.path.realpath('__file__'))
parent_directory = parent = os.path.dirname(current_notebook_path)

sys.path.append(parent_directory)

# COMMAND ----------

# MAGIC %md Import tests and test runner

# COMMAND ----------

from tests import TestStringMethods, TestSparkMethods
from test_runner import apply_tests

# COMMAND ----------

# MAGIC %md Execute tests

# COMMAND ----------

tests = [TestStringMethods, TestSparkMethods]
results = apply_tests(tests)

# COMMAND ----------

results.pandas_df

# COMMAND ----------

out = results.bytes_output
out.seek(0)
dbutils.notebook.exit(out.read().decode('utf-8'))
