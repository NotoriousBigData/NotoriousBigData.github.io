rom pyspark.sql import SparkSession
spark = SparkSession.builder.appName("appName").getOrCreate()
sc = spark.sparkContext
data = sc.textFile("./flights.csv")
header = data.first()
data = data.filter(lambda line: line != header)
data = data.map(lambda x: x.split(','))
data = data.map(lambda vuelo: (vuelo[1],vuelo[2],vuelo[7],vuelo[8],vuelo[11],vuelo[22])) #todos los vuelos
events = sc.textFile("./events.csv")
events = events.map(lambda x: x.split(','))

def getVuelosEvento(vuelo,evento):
	if vuelo[0] == evento[0] and vuelo[1] == evento[1] and (vuelo[2] == evento[3] or vuelo[3]==evento[3]):
		return 1
	else:
		return 0

def getVuelosAeropuerto(vuelo,aeropuerto):
	if (aeropuerto == vuelo[2] or aeropuerto==vuelo[3]):
		return 1
	else:
		return 0

def isFracaso(vuelo): # Esta funcion mira si el vuelo no se ha conseguido, es decir, se ha cancelado o no ha llegado
	if vuelo[4]=='' or vuelo[5]=='':
		return 1
	else:
		return 0

def isDelayed(vuelo): # Esta funcion mira si el vuelo ha salido o ha llegado con retraso
	if float(vuelo[4])>0 or float(vuelo[5])>0:
		return 1
	else:
		return 0


dataC = data.filter(lambda vuelo: isFracaso(vuelo) == 1) #vuelos cancelados o con destino alterado
dataV = data.filter(lambda vuelo: isFracaso(vuelo) == 0) #vuelos con exito
dataVR = dataV.filter(lambda vuelo: isDelayed(vuelo) == 0) #vuelos con retraso

def eventualidad(evento):
	print "cuando sucedio el evento " + evento[2] + " en el dia " + evento[1] + " del mes " + evento[0] +" de 2015"
	print "sucedio lo siguiente en el aeorpuerto "+evento[3]
	print ""
	eventoV = data.filter(lambda v : getVuelosEvento(v,evento))
	n_eventoV = eventoV.count()
	print "Una cantidad de " + str(n_eventoV) + " vuelos planeados, siendo destino o origen"
	eventoVC = dataC.filter(lambda v : getVuelosEvento(v,evento))
	n_eventoVC = eventoVC.count()
	print "Una cantidad de " + str(n_eventoVC) + " vuelos que fracasaron, siendo destino o origen"
	print "Es decir que no llegaron a su destino o no llegaron a salir"
	print ""
	eventoVE = dataV.filter(lambda v : getVuelosEvento(v,evento))
	n_eventoVE = eventoVE.count()
	print "Una cantidad de " + str(n_eventoVE) + " vuelos que llegaron, siendo destino o origen"
	print "sin tener en cuenta el retraso"
	print ""
	eventoVR = dataVR.filter(lambda v : getVuelosEvento(v,evento))
	n_eventoVR = eventoVR.count()
	print "Una cantidad de " + str(n_eventoVR) + " vuelos que tuvieron algun retraso, siendo destino o origen"
	print ""
	return (evento[0],evento[1],evento[2],evento[3],n_eventoV,n_eventoVE,n_eventoVC,n_eventoVR)

e1 = events.first()
eventualidad(e1);
