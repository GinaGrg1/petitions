import re

from pyspark.sql import DataFrame
from pyspark.sql.window import *
from pyspark.sql.functions import col, monotonically_increasing_id, lit, regexp_count, lower


class FilePathNotFoundError(Exception):
    pass

def read_in_json(file_path: str, spark_:object=None) -> DataFrame:
    """
    Args:
        file_path: str
            Full path of the json file.
        spark_: object
            Optional parameter. This is only because the unit testing code is not using databricks..

    Returns:
        Spark dataframe
    """
    _spark = spark_ if spark_ else spark
    
    try:
        data = _spark.read.option("multiline","true")\
                    .json(file_path)\
                        .drop(*['label', 'numberOfSignatures'])\
                            .withColumn("abstract", col("abstract")["_value"])\
                                .withColumn("petition_id", monotonically_increasing_id()+1)
        return data
    except FilePathNotFoundError:
        raise f"File does not exist in given file path: {file_path}"
    

def get_top_20_columns(df: DataFrame) -> list:
    """
    Function to count the top 20 most repeated words in a petition description. 

    Args:
        df: DataFrame
    
    Returns:
        list of top 20 mostly occurred words.
    
    The final list of tuple will look something like this:
        [('should', 3171),
            ('people', 2494),
            ('government', 1922),
            ('children', 975),
            ('public', 834),....]
    """
    top_20_cols_list = data.select('abstract').rdd.flatMap(lambda line: line.abstract.split(' '))\
                        .filter(lambda word: re.match(r"^[a-zA-Z]{6,}",word))\
                            .map(lambda x: (x.lower().replace('\n', ''), 1))\
                                .reduceByKey(lambda x, y: x+y)\
                                    .sortBy(lambda counts: counts[1], ascending=False).collect()[0:19]

    return [item[0] for item in top_20_cols_list]

def write_out_csv(df: DataFrame, cols_list: list, output_path: str) -> None:
  """
  Function to write out a spark dataframe into a csv file.

  Args:
    df : DataFrame
      Dataframe to be written out. 
    cols_list: list
      Top 20 most occurred words in the petitions. These will be created as new columns
    output_path: str
      Path to write out the csv file to.

  Returns:
    None
  """
  for column in cols_list:
      df = df.withColumn(column, regexp_count(lower('abstract'), lit(r'\b' + column + r'\b')))
  
  df.drop("abstract").coalesce(1).write.csv(output_path, header=True)

if __name__ == "__main__":
  
  df = read_in_json("../input/input_data.json)
  columns_list = get_top_20_columns(df)

  write_out_csv(df, columns_list, "../output/output_data.csv")


