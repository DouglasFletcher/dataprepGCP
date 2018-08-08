
import censusgeocode as cg
import os
import time
import cloudstorage as gcs

# globals:
BUCKET_NAME = os.environ.get('BUCKET')
PROJECT_ID = os.environ.get('PROJECT')

def geocodeOhneSpark(fileDir, fileName, colX, colY):
	# file reference
	fileRef = fileDir + fileName
	outfile = open(fileRef + "_complete" + ".csv","w") 
	with open(fileRef + ".csv", "r") as ins:
		errorCnt = 0
		for i, line in enumerate(ins):
			# logging
			if i % 50 == 0:
				print("number of geocoded rows: %s" % i)
			# get getoid with coordinates
			if i == 0:
				xcordPos = line.split(",").index(colX)
				ycordPos = line.split(",").index(colY)
				outfile.write(line.replace('\n', '')+",GEOID\n")
			else:
				xcord = line.split(",")[xcordPos]
				ycord = line.split(",")[ycordPos]
				try:
					result = cg.coordinates(x=xcord, y=ycord)
					outfile.write(line.replace('\n', '')+","+result["2010 Census Blocks"][0]["GEOID"] + "\n")
				except ValueError:
					print("skipping line %s" % i)
					errorCnt += 1
	print("missed lines - parse errors %s" % errorCnt)
	ins.close()
	outfile.close()


def geocodeOhneSparkGcp(fileDir, fileName, colX, colY):
	# file reference
	readFile = 'gs://{}/tables/{}/{}'.format(BUCKET_NAME, fileDir, fileName+".csv")
	writFile = 'gs://{}/tables/{}/{}'.format(BUCKET_NAME, fileDir, fileName+"_complete"+".csv")
	# save destination
	outfile = gcs.open(writFile,'w', content_type='text/plain')
	# contents destination
	gcs_file = gcs.open(readFile)
	contents = gcs_file.read()
	gcs_file.close()
	# geocode & save data
	with contents as ins:
		errorCnt = 0
		for i, line in enumerate(ins):
			# logging
			if i % 50 == 0:
				print("number of geocoded rows: %s", i)
			# get getoid with coordinates
			if i == 0:
				xcordPos = line.split(",").index(colX)
				ycordPos = line.split(",").index(colY)
				outfile.write(line.replace('\n', '')+",GEOID\n")
			if 0 < i < 10:
				xcord = line.split(",")[xcordPos]
				ycord = line.split(",")[ycordPos]
				try:
					result = cg.coordinates(x=xcord, y=ycord)
					outfile.write(line.replace('\n', '')+","+result["2010 Census Blocks"][0]["GEOID"] + "\n")
				except ValueError:
					print("skipping line %s" % i )
					errorCnt += 1
	print('Exported to {}.{}'.format(PROJECT_ID, destinationUri))
	print("missed lines - parse errors %s" % errorCnt )
	outfile.close()



def geocodeMitSpark(rdd, colXCord, colYCord):
	"""
	geocode cloud storage data with x,y coordinates to give geoid e.g. zipcode
	"""
	# instantiate spark instance
	#spark = SparkSession\
	#	.builder\
	#	.appName("geocoding")\
	#	.getOrCreate()
	#spark.stop()
	# read file as pointer value

	# go through each row in file and merge geoId, with new names

	# override file with same reference
	result = cg.coordinates(x=-76, y=41)
	print(result)


# main 
if __name__ == '__main__':
	t = time.clock()
	#cwd = os.getcwd()
	#geocodeOhneSpark(cwd + "/testdata/","test","longitude","latitude")
	geocodeOhneSparkGcp("statmetadata", "output","longitude","latitude")	
	print(format(time.clock()-t, ".2f"))



