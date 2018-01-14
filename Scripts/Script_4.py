from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Script4").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diasTiempos = vuelos.map(lambda line:line.split(","))
diasTiempos2 = diasTiempos.map(lambda line:(str(line[8]),(str(line[11]),str(line[22]))))
diasTiempos3 = diasTiempos2.filter(lambda line: len(line[1][0])>0 and len(line[1][1])>0)
def isDelayed(x,y):
	if x > 0 or y > 0:
		return 1
	else:
		return 0

diasRetraso = diasTiempos3.map(lambda line:( line[0], isDelayed(float(line[1][0]),float(line[1][1]))))
diasRetrasoMax = diasRetraso.reduceByKey(lambda a,b: a + b)
diasRetrasoMax = diasRetrasoMax.map(lambda line: (line[1],line[0]))
diasRetrasoMax = diasRetrasoMax.sortByKey(ascending=False)
maxDiaRetrasado = diasRetrasoMax.first()
print("\n\nEl destion que mas se retrasa es: " +  str(diasRetrasoMax.first()) + "\n\n")


