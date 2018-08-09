
import censusgeocode as cg
import os
import time

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

# main 
if __name__ == '__main__':
	t = time.clock()
	cwd = os.getcwd()
	geocodeOhneSpark(cwd + "/statmetadata/","output","longitude","latitude")
	#geocodeOhneSpark(cwd + "/statmetadata/","output","longitude","latitude")
	#geocodeOhneSpark(cwd + "/statmetadata/","output","longitude","latitude")

	print(format(time.clock()-t, ".2f"))



