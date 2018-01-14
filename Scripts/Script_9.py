from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diasTiempos = vuelos.map(lambda line:line.split(","))




diasTiempos2 = diasTiempos.map(lambda line: (("AS",str(line[26])), ("SC",str(line[27])), ("AL",str(line[28])), ("LA",str(line[29])), ("WE",str(line[30]))))
diasTiempos3 = diasTiempos2.filter(lambda line: len(line[0][1])>0 and len(line[1][1])>0 and len(line[2][1])>0 
and len(line[3][1])>0 and len(line[4][1])>0)
diasTiempos3 = diasTiempos3.map(lambda line: (("AS",float(line[0][1])), ("SC",float(line[1][1])), ("AL",float(line[2][1])), ("LA",float(line[3][1])), ("WE",float(line[4][1]))))
def isDelayed(x):
	
	if x > 0:
		return 1
	else:
		return 0




AS = diasTiempos3.map(lambda line: (isDelayed(line[0][1])))
SC = diasTiempos3.map(lambda line: (isDelayed(line[1][1])))
AL = diasTiempos3.map(lambda line: (isDelayed(line[2][1])))
LA = diasTiempos3.map(lambda line: (isDelayed(line[3][1])))
WE = diasTiempos3.map(lambda line: (isDelayed(line[4][1])))

totalDelays = AS.count()

AS = AS.reduce(lambda x,y: x + y)
SC = SC.reduce(lambda x,y: x + y)
AL = AL.reduce(lambda x,y: x + y)
LA = LA.reduce(lambda x,y: x + y)
WE = WE.reduce(lambda x,y: x + y)

result = [((AS * 100) / totalDelays), ((SC * 100) / totalDelays),((AL * 100) / totalDelays), ((LA * 100) / totalDelays), ((WE * 100) / totalDelays)]

print("Los resultados son : " + "AS-> " +  str(result[0]) + "% SC-> " +  str(result[1]) + "% AL-> " +  str(result[2])
+ "% LA-> " +  str(result[3]) + "% WE-> " +  str(result[4]) + "%")


