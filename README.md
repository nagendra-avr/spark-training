
Spark-Training

Python Execution

export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

spark-submit  --py-files spark-training-0.0.1-SNAPSHOT-jar-with-dependencies.jar --master local /home/nagi/Learning/Spark/spark-training/src/main/python/py/SparkTest.py