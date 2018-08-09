
from geopy.geocoders import Nominatim
import os
import time
from geopy.exc import GeocoderServiceError


def geocodeOhneSpark(fileDir, fileName, colX, colY):
	# file reference
	fileRef = fileDir + fileName
	print("geocoding file: {}".format(fileRef))
	outfile = open(fileRef + "_geocoded" + ".csv","w") 
	# ref object for geocoding
	geolocator = Nominatim(user_agent="geocoding_qlik")
	with open(fileRef + ".csv", "r") as ins:
		errorCnt = 0
		for i, line in enumerate(ins):
			lineClean = line.replace('\n', '') 
			# logging
			if i % 50 == 0:
				print("number of geocoded rows: %s" % i)
			# get getoid with coordinates
			if i == 0:
				xcordPos = lineClean.split(",").index(colX)
				ycordPos = lineClean.split(",").index(colY)
				outfile.write(lineClean+",postcode\n")
			else:
				xcord = lineClean.split(",")[xcordPos]
				ycord = lineClean.split(",")[ycordPos]
				try:
					request = str(xcord) + ", " + str(ycord)
					response = geolocator.reverse(request)
					outfile.write(lineClean+","+response.raw["address"]["postcode"]+"\n")
					time.sleep(1)
					# API can handle certain request limit / sec.
					if i % 150 == 0:
						time.sleep(150)
				except GeocoderServiceError:
					print("skipping line %s" % i)
					errorCnt += 1
					time.sleep(150)


	print("missed lines - parse errors %s" % errorCnt)
	ins.close()
	outfile.close()

# main 
if __name__ == '__main__':
	t = time.clock()
	cwd = os.getcwd()

	geocodeOhneSpark(cwd + "/weathermeta/","output","lat","lon")
	print(format(time.clock()-t, ".2f"))

	geocodeOhneSpark(cwd + "/statmetadata/","output","latitude","longitude")
	print(format(time.clock()-t, ".2f"))