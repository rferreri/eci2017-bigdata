from __future__ import print_function

import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":

	FILENAME = "input/touristData.csv"

	spark = SparkSession.builder.appName("TouristsByCountry").getOrCreate()
	tourists_csvFile = spark.read.csv(FILENAME)

	tourists_array = tourists_csvFile.rdd.map(lambda x: (str(x[1]), int(x[6])))\
										 .reduceByKey(lambda x, y: x + y)\
										 .map(lambda (x, y): (y, x))\
										 .sortByKey(False)\
										 .map(lambda (x, y): (y, x))\
										 .map(lambda (x, y): "%s %d" % (x, y))

	tourists_array.saveAsTextFile("output")

	spark.stop()