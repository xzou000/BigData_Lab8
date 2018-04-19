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
	NYC_rest='nyc_restaurants.csv'

	rest = sc.textFile(NYC_rest, use_unicode=False).cache()	
	rest_cuisin = rest.mapPartitionsWithIndex(extractREST)
	result=rest_cuisin.reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1])
	print result.take(10)
	
if __name__ == "__main__":
	sc = SparkContext()
	# Execute Main functionality
	main(sc)