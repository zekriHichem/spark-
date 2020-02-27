

import os 


str = os.system('./spark-submit --class org.apache.spark.examples.SparkPi --master spark://spark:7077 $SPARK_HOME/examples/jars/spark-examples*.jar 100')

print(str)
print("hello")
