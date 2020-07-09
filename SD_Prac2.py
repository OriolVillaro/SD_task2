import pywren_ibm_cloud as pywren
import json
from random import randint
from datetime import datetime
import time
from time import sleep
import sys


x=1
bucket = 'sd-ori-un-buen-cubo'

def master(id, x, ibm_cos): 
	write_permission_list = [] #llista que farem servir per controlar l'ordre en que 
	#master dona permís als slaves
	noMesPeticions=False
	ibm_cos.delete_object(Bucket=bucket, Key="a.txt")
	#ibm_cos.put_object(Bucket='eduard-bucket', Key="update.txt")

	ibm_cos.put_object(Bucket=bucket, Key="a.txt")
	#ibm_cos.delete_object(Bucket='eduard-bucket', Key="result.txt")

	ibm_cos.put_object(Bucket=bucket, Key="result.txt")	#creem el fitxer
	dataAntiga=dataActual=ibm_cos.head_object(Bucket=bucket, Key="result.txt")['LastModified']

	#que acturà com secció crítica
	hiHaPeticions=False	#variable que controla si hi ha peticions de modificació
	while(hiHaPeticions==False):
		try:
			object_names=ibm_cos.list_objects(Bucket=bucket, Prefix='p_write_')['Contents']
			#els ordenem per data de creació
			hiHaPeticions=True
		except:
			sleep(x)
	hiHaPeticions=False
	#time.sleep(x)
	while(noMesPeticions==False):	#mentre tinguem peticions	
		object_names=sorted(object_names, key=lambda i: i['LastModified'])
		#prenem l'element més antic de la llista
		mesAntic=object_names.pop()['Key']

		#prenem el numero identificador de l'slave
		idSlave=mesAntic[8:]
		ibm_cos.put_object(Bucket=bucket, Key="write_"+str(idSlave))
		ibm_cos.delete_object(Bucket=bucket, Key=mesAntic)

		#pujem el fitxer de permís de modificació
		#afegim l'ID de l'slave a la llista

		write_permission_list.append(idSlave)
		#esborrem el fitxer de petició del COS
		sleep(x)
		
		#resultat=ibm_cos.get_object(Bucket='eduard-bucket', Key="a.txt")['Body'].read()
		#resultat=(resultat.decode())+"master: vaig a buscar update de "+str(idSlave)+"data actual de result: "+str(dataAntiga)+'\n'
		#ibm_cos.put_object(Bucket='eduard-bucket', Key="a.txt", Body=resultat)
		while(dataActual==dataAntiga):	#mentre no s'hagi actualitzat
			#tornem a comprovar la data
			dataActual=ibm_cos.head_object(Bucket=bucket, Key="result.txt")['LastModified']
			sleep(0.2)
			#resultat=ibm_cos.get_object(Bucket='eduard-bucket', Key="update.txt")['Body'].read()
			#resultat=(resultat.decode())+"master: data actual de result "+str(dataAntiga)+'\n'
			#ibm_cos.put_object(Bucket='eduard-bucket', Key="update.txt", Body=resultat)

		#resultat=ibm_cos.get_object(Bucket='eduard-bucket', Key="result.txt")['Body'].read()
		#resultat=(resultat.decode())+"master: "+str(idSlave)+" ha actuaitzat el fitxer"+'\n'
		#ibm_cos.put_object(Bucket='eduard-bucket', Key="result.txt", Body=resultat)
		dataAntiga=dataActual
		ibm_cos.delete_object(Bucket=bucket, Key="write_"+str(idSlave))

		#resultat=ibm_cos.get_object(Bucket='eduard-bucket', Key="a.txt")['Body'].read()
		#resultat=(resultat.decode())+"master: updated, data actual de result: "+str(dataAntiga)+'\n'
		#ibm_cos.put_object(Bucket='eduard-bucket', Key="a.txt", Body=resultat)
		#esborrem l'objecte de permís
		sleep(x)
		try:
			object_names=ibm_cos.list_objects(Bucket=bucket, Prefix='p_write_')['Contents']
		except:
			noMesPeticions=True
			resultat=ibm_cos.get_object(Bucket=bucket, Key="a.txt")['Body'].read()
			resultat=(resultat.decode())+"master: no trobo mes peticions"+'\n'
			ibm_cos.put_object(Bucket=bucket, Key="a.txt", Body=resultat)
		
	return write_permission_list


def slave(id, x, ibm_cos): 
	id+=1
	ibm_cos.put_object(Bucket=bucket,Key="p_write_"+str(id))	# escrivim
	# el fitxer de petició de l'slave
	tePermis=False
	#time.sleep(x)
	while(tePermis==False):	#mentre no tingui permís, espera i torna a comprovar
		try:
			resultat=ibm_cos.get_object(Bucket=bucket, Key="write_"+str(id))
			tePermis=True
		except:
			sleep(x)
	sleep(2)
	#aa=ibm_cos.get_object(Bucket='eduard-bucket', Key="a.txt")['Body'].read()
	#aa=(aa.decode())+"slave "+str(id)+" ha trobat el fitxer de permis"+'\n'
	#ibm_cos.put_object(Bucket='eduard-bucket', Key="a.txt", Body=aa)
	#quan té permís fa get del fitxer "resultats.txt"
	resultat=ibm_cos.get_object(Bucket=bucket, Key="result.txt")['Body'].read()
	#el decodifiquem i hi afegim l'ID de l'slave
	resultat=(resultat.decode())+str(id)+'\n'
	
	#pujem el "result.txt" modificat al COS
	ibm_cos.put_object(Bucket=bucket, Key="result.txt", Body=resultat)

	aa=ibm_cos.get_object(Bucket=bucket, Key="a.txt")['Body'].read()
	aa=(aa.decode())+"slave "+str(id)+" ha modificat i pujat el fitxer"+'\n'
	ibm_cos.put_object(Bucket=bucket, Key="a.txt", Body=aa)
	return None

if  __name__ == '__main__':

	start_time=time.time()
	nSlaves=int(sys.argv[1])	#prenem el nombre d'slaves passats per paràmetre
	N_SLAVES=nSlaves
	
	"""try:"""
	pw = pywren.ibm_cf_executor()
	
	pw.call_async(master, 0)

	pw.map(slave, range(N_SLAVES))
	write_permission_list = pw.get_result()
	"""except:""
		print("F, CREDENCIALS INVALIDES O ALGO")"""
	print(str(write_permission_list)+'\n')
	ibm_cos=pw.internal_storage.get_client()

	resultat=ibm_cos.get_object(Bucket=bucket, Key="result.txt")['Body'].read().decode()
	print (resultat+"\n\n\n")
	
	aa=ibm_cos.get_object(Bucket=bucket, Key="a.txt")['Body'].read().decode()
	print (aa)

	content=ibm_cos.list_objects_v2(Bucket=bucket)['Contents']

	print("------%s segons ------" % (time.time()-start_time))

	
	
	resultat = resultat.split("\n")
	resultat.remove("")
	
	if (write_permission_list[0] == resultat):    
		print("\nLes dues llistes són iguals. Execució correcta.\n")
	else: print("\nERROR. Execució incorrecta.\n")
	

