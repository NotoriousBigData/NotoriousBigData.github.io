from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
vuelos = sc.textFile("./flights.csv")
header = vuelos.first()
vuelos = vuelos.filter(lambda line: line != header)
diasTiempos = vuelos.map(lambda line:line.split(","))
#Dejar cabecera, elimina el header y  separa en columnas.


#Calculamos num de retrasos por aeropuerto.
diasTiempos2 = diasTiempos.map(lambda line: ((str(line[7]),1),(str(line[7]), str(line[11]))))
diasTiempos2 = diasTiempos2.filter(lambda line: len(line[0]) == 3)



diasTiempos3 = diasTiempos2.map(lambda line: ((line[0], 1)))
originDelays = diasTiempos3.reduceByKey(lambda a,b: a + b)
originDelays = originDelays.map(lambda line: ((line[1],line[0])))

#Calculamos los vuelos x aeropuerto.
originFlights = diasTiempos.map(lambda line: (str(line[7]), 1))
originFlights = originFlights.filter(lambda line: len(line[0]) == 3)
originFlights = originFlights.reduceByKey(lambda a,b: a + b)






originDelays = originDelays.sortByKey(lambda x,y: x >y)
lessDelay = originDelays.take(5)

originDelays = originDelays.sortByKey(False)
mostDelay = originDelays.take(5)

print("Menos retrasados\t" + str(lessDelay))
print("\nMas retrasados\t" + str(mostDelay))

