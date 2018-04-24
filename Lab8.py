'''
from pyspark import SparkContext
def extractREST(index, lines):
    import csv
    if index==0:
        lines.next()
    reader = csv.reader(lines)
    for row in reader:
        cuisin = row[7]
        yield (cuisin, 1)
		
def main(sc):
	NYC_rest='/data/share/bdm/nyc_restaurants.csv'
	rest = sc.textFile(NYC_rest, use_unicode=False).cache()	
	rest_cuisin = rest.mapPartitionsWithIndex(extractREST)
	result=rest_cuisin.reduceByKey(lambda x,y: x+y).orderBy(lambda x: -x[1])
	print result.take(10)
	
if __name__ == "__main__":
	sc = SparkContext()
	# Execute Main functionality
	main(sc)
'''
from pyspark import SparkContext 
from pyspark.sql import SparkSession

if __name__ == "__main__":
	spark = SparkSession(SparkContext())
	df = spark.read.load('nyc_restaurants.csv', format = 'csv',
							   header=True, inferSchema=True)
	df = df.groupBy(['CUISINE DESCRIPTION']).count()
	df = df.orderBy('count', ascending = False)
	df.show()