
import censusgeocode as cg
from pyspark.sql import SparkSession



def geocodePoint(rdd, colXCord, colYCord):
	"""
	geocode cloud storage data with x,y coordinates to give geoid e.g. zipcode
	"""
	# read file as pointer value

	# go through each row in file and merge geoId, with new names

	# override file with same reference
	result = cg.coordinates(x=-76, y=41)
	print(result)


# main 
if __name__ == '__main__':

	# instantiate spark instance
	spark = SparkSession\
		.builder\
		.appName("geocoding")\
		.getOrCreate()

	geocodePoint("","","")

	spark.stop()


