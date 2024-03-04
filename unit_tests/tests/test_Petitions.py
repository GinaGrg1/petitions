import sys
import os

sys.path.append(f"{os.getcwd()}/src")

import pytest
import unittest

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from framework_functions import read_in_json, get_top_20_columns
import framework_functions

@pytest.fixture(autouse=True)
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
            .appName("Unit-Testing-Petitions")
            .config("spark.executor.cores", "1")
            .getOrCreate()
    )

    yield spark
    spark.stop()

@pytest.mark.usefixtures("spark")
class TestPetitions:
      INPUT_PATH = f"{os.getcwd()}/data/input_data_test.json"

      def test_read_in_json(self, spark):
        
        df = read_in_json(self.INPUT_PATH, spark)

        assert df.count() > 0
        assert len(df.columns) == 3

    
        
      

if __name__ == '__main__':
    unittest.main()
