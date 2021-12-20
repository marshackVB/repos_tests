import unittest
import xml.etree.ElementTree as ET
import pandas as pd
import xmlrunner
import uuid
import io
import datetime
import json
from collections import namedtuple


def get_xml_test_results(list_of_tests):
  """
  Run tests using the xmlrunner package, return tests' return results as
  a binary object and display results
  """
  
  loader = unittest.TestLoader()
  suite = unittest.TestSuite()
 
  for test_class in list_of_tests:
    tests = loader.loadTestsFromTestCase(test_class)
    suite.addTests(tests)
    
  out = io.BytesIO()
  runner = xmlrunner.XMLTestRunner(out)
  return (out, runner.run(suite))


def convert_bytes_to_df(out):

  out.seek(0)
  test_results = ET.XML(out.read().decode('utf-8'))

  ts = []
  for suite in test_results:
    for test in suite:
      failures = [{k:v for k,v in failure.items()} for failure in test]
      if len(failures) > 0:
        for failure in failures:
          attributes = {k:v for k,v in suite.attrib.items()}
          attributes.update({f"test_{k}":v for k,v in test.attrib.items()})
          attributes.update({f"failure_{k}":v for k,v in failure.items()})
          ts.append(attributes)
      else:
        attributes = {k:v for k,v in suite.attrib.items()}
        attributes.update({f"test_{k}":v for k,v in test.attrib.items()})
        attributes.update({"failure_type":None, "failure_message":None})
        ts.append(attributes)

  # Convert to parsed XML to a Pandas Dataframe
  df = pd.DataFrame(ts)
 
  df["tests"] = df["tests"].astype(int)
  df["errors"] = df["errors"].astype(int)
  df["failures"] = df["failures"].astype(int)
  df["skipped"] = df["skipped"].astype(int)
  df["succeeded"] = df["tests"] - (df["errors"] + df["failures"] + df["skipped"])
  df["name"] = df["name"].apply(lambda x: str.join("-", x.split("-")[:-1]))
  
  # Removed timestamp, time, test_line columns
  df = df.loc[:, ["name", "tests", "succeeded", "errors", "failures", 
                  "skipped", "test_name", "test_time", "failure_type", 
                  "failure_message"]]
  

  return df


def apply_tests(list_of_tests):
  """
  Requires a list of test classes that inherit from unittest.TestCase. Will run
  all tests, output in byte format, xml, and a Pandas dataframe
  
  Example: class TestStringMethods(unittest.TestCase)
  """
  
  out, results = get_xml_test_results(list_of_tests)
  
  df = convert_bytes_to_df(out)
  
  TestResults = namedtuple('TestResults', 'bytes_output, xml_output, pandas_df')
  
  return TestResults(out, results, df)