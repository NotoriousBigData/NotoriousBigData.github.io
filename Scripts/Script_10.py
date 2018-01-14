from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diasTiempos = vuelos.map(lambda line:line.split(","))
#Dejar cabecera, elimina el header y  separa en columnas.

diasTiempos2 = diasTiempos.map(lambda line: (float(line[24]), str(line[25])))


diasTiempos3 = diasTiempos2.filter(lambda line: line[0] > 0)
diasTiempos3 = diasTiempos3.map(lambda line: ((line[1], line[0])))




canceledReason = diasTiempos3.reduceByKey(lambda a,b: a + b)
array = canceledReason.collect()


totalCancelation = canceledReason.map(lambda line: (line[1]))
totalCancelation = totalCancelation.reduce(lambda x,y: x + y)

result = [((array[0][1] * 100) / totalCancelation), ((array[1][1] * 100) / totalCancelation),((array[2][1] * 100) / totalCancelation), ((array[3][1] * 100) / totalCancelation)]

print("Los resultados son : " + array[0][0] + "\t" +  str(result[0]) + "%, " + array[1][0] + "\t" + str(result[1]) + "%, " + array[2][0] + "\t" +  str(result[2])
+ "%, " + array[3][0] + "\t" +  str(result[3]) + "%")


