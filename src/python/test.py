import pyspark

import os, sys

# Configure the python executable for PySpark workers to use.
#
# Without this, PySpark fails with:
# Exception: Python in worker has different version 2.7 than that in driver 3.7, PySpark cannot run with different minor versions.Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
os.environ['PYSPARK_PYTHON'] = sys.executable

# Configure the paths PySpark workers will use when searching for packages to import.
#
# Without this, the function `f` below fails with:
# ModuleNotFoundError: No module named 'numpy'
os.environ['PYTHONPATH'] = ":".join(sys.path)

def run():
    def f(x):
        import numpy
        return x


    # create a spark context using a local cluster with 2 workers
    conf = pyspark.SparkConf().setMaster("local[2]")
    sc = pyspark.SparkContext.getOrCreate(conf)

    result = sc.parallelize("abcde").map(lambda x: f(x)).collect()

    assert result == ['a', 'b', 'c', 'd', 'e']

run()
