
import censusgeocode as cg
import os
import time

def geocodeOhneSpark(fileDir, fileName, colX, colY):
	# file reference
	fileRef = fileDir + fileName
	outfile = open(fileRef + "_geocoded" + ".csv","w") 
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
				outfile.write(lineClean+",GEOID\n")
			else:
				xcord = lineClean.split(",")[xcordPos]
				ycord = lineClean.split(",")[ycordPos]
				try:
					result = cg.coordinates(x=xcord, y=ycord)
					outfile.write(lineClean+","+result["2010 Census Blocks"][0]["GEOID"] + "\n")
				except ValueError:
					print("skipping line %s" % i)
					errorCnt += 1
	print("missed lines - parse errors %s" % errorCnt)
	ins.close()
	outfile.close()

# main 
if __name__ == '__main__':
	t = time.clock()
	cwd = os.getcwd()
	geocodeOhneSpark(cwd + "/weathermeta/","output","lon","lat")
	geocodeOhneSpark(cwd + "/statmetadata/","output","longitude","latitude")

	print(format(time.clock()-t, ".2f"))



