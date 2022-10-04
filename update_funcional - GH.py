from datetime import timedelta
from datetime import datetime
import teradatasql
import queue
import re
import requests
from threading import Thread
import pandas as pd
from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook

DEFAULT_ARGS = {
	'owner': 'airflow',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
	}

# ------- DAG builder -------

@dag(dag_id='Actualizacion_Teradata',
	 default_args=DEFAULT_ARGS,
	 start_date=datetime(2022, 9, 12),
	 catchup=False,
	 schedule_interval='@daily',
	 tags=['AFIP', 'Teradata', 'Update'])


def my_dag():
	@task
	def generador_cuit():
		"""
		Extracts 'CUIT' values that needs to be updated from the db 
		and creates an array with 4 lists with them.
		:return: array
		"""
		conn = BaseHook.get_connection(f'Teradata')
		con_tera = teradatasql.connect(None,
					       host='localhost',
					       user=f"{conn.login}",
					       password=f"{conn.password}",
					       column_name='false')
		tsql = con_tera.cursor()

		query = ("SELECT top 500 CUIT TABLE_TO_UPDATE WHERE FUENTE = 'old_data'")

		lista_final = []

		for _ in range(4):
			tsql.execute(query)
			col_headers = [i[0] for i in tsql.description]
			rows = [list(i) for i in tsql.fetchall()]
			data = pd.DataFrame(rows, columns=col_headers)

			cuit_lista = []
			for d in data['CUIT']:
				cuit_lista.append(d)

			lista_final.append(cuit_lista)

		tsql.close()

		for l in lista_final:
			for n in l:
				if n == '':
					l.remove('')

		print('CONSULT LIST: \n', lista_final)
		return lista_final

	@task
	def consulta_afip(listas_cuits):
		"""
		:param: list for the dynamic iteration
		Makes an API call with the list values and save them into a new list.
		"""
		class Worker(Thread):
			def __init__(self, request_queue):
				Thread.__init__(self)
				self.queue = request_queue
				self.results = []

			def run(self):
				while True:
					content = self.queue.get()
					if content == "":
						break
					response = requests.get(url=content).json()
					self.results.append(response)
					self.queue.task_done()

		# Creates the queue and concatenates the number at the end of the url
		q = queue.Queue()
		for cuit in listas_cuits:
			q.put({AFIP_URL} + re.sub("[^0-9]", "", cuit))

		# Workers keep working till they receive an empty string
		no_workers = 8
		for _ in range(no_workers):
			q.put("")

		# Create workers and add tot the queue
		workers = []
		for _ in range(no_workers):
			worker = Worker(q)
			worker.start()
			workers.append(worker)
		# Join workers to wait till they finished
		for worker in workers:
			worker.join()

		# Combine the results get from workers, and appends to the list
		r = []  # (jsons list)
		for worker in workers:
			r.extend(worker.results)

		return r

	@task
	def update_teradata(lista):
		"""
		:param list for the dynamic iteration
		Builds a df with the list, creates the query and executes it
		obtaining the data (values) from columns (keys) to update Teradata database.
		"""

		conn = BaseHook.get_connection(f'Teradata')
		con_tera = teradatasql.connect(None,
					       host='localhost',
					       user=f"{conn.login}",
					       password=f"{conn.password}",
					       column_name='false')

		tsql = con_tera.cursor()

		for c in lista: # c = every list that contains 500 consults
			df = pd.DataFrame(c, columns=['errorGetData','Contribuyente'])
			df_consulta = df['Contribuyente']
			df_consulta = df_consulta.dropna(how='all')
			for j in df_consulta: # j = every row (json)
				domicilio = j.get('domicilioFiscal')
				DIRECCION = domicilio.get('direccion')
				CODIGO_POSTAL = domicilio.get('codPostal')
				PROVINCIA = domicilio.get('nombreProvincia')
				LOCALIDAD = domicilio.get('localidad')
				SITUACION_JURIDICA = j.get('tipoPersona')
				ESTADO = j.get('estadoClave')
				NOMBRE_COMPLETO = j.get('nombre')
				CUIT = j.get('idPersona')

				comilla = "'"
				if comilla in NOMBRE_COMPLETO:
					NOMBRE_COMPLETO = NOMBRE_COMPLETO.replace(comilla, "''")

				query_act = (f"UPDATE TABLE_TO_UPDATE SET DIRECCION = '{DIRECCION}', "
					     f"SITUACION_JURIDICA = '{SITUACION_JURIDICA}', "
					     f"ESTADO = '{ESTADO}', "
					     f"NOMBRE_COMPLETO = '{NOMBRE_COMPLETO}', "
					     f"PROVINCIA = '{PROVINCIA}', "
					     f"LOCALIDAD = '{LOCALIDAD}', "
					     f"CODIGO_POSTAL = '{CODIGO_POSTAL}', "
					     f"FUENTE = 'AFIP_TANGO_FACTURA', "
					     f"FECHA_ACTUALIZACION = current_date "
					     f"WHERE CUIT = '{CUIT}'")

				# This way, Teradata'll ingest the value as NULL and not string:
				comillas_none = "'None'"
				nulos = "NULL"
				query_final = query_act.replace(comillas_none, nulos)
				print(f'THE UPDATED NUMBERS ARE: {CUIT}')
				tsql.execute(query_final)
		tsql.close()

	l = consulta_afip.expand(listas_cuits=generador_cuit())
	update_teradata(l)

act_teradata = my_dag()
