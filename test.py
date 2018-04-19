## Imports
from pyspark import SparkContext
## Main functionality
def main(sc):
	rdd = sc.parallelize(range(1000), 10)
	print rdd.mean()
if __name__ == "__main__":
	sc = SparkContext()
	# Execute Main functionality
	main(sc)