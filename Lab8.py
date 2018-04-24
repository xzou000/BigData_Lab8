from pyspark import SparkContext 
from pyspark.sql import SparkSession

if __name__ == "__main__":
	spark = SparkSession(SparkContext())
	df = spark.read.load('/data/share/bdm/nyc_restaurants.csv', format = 'csv',
							   header=True, inferSchema=True)
	df = df.groupBy(['CUISINE DESCRIPTION']).count()
	df = df.orderBy('count', ascending = False)
	df.saveTextFile('output')
