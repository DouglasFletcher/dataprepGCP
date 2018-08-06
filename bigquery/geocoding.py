import censusgeocode as cg

def geocodePoint(dirLoc, fileName, colXCord, colYCord):
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
	geocodePoint("","","","")


