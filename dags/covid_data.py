import requests
import os
#from multiprocessing.pool import ThreadPool
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import date, datetime, timedelta
import psycopg2
import csv
import pandas as pd

# Intialinzing the global variables
list_file_names=[]
local_dir_path='/usr/local/airflow/covid_data/'
today=date.today()
today=today.strftime('%d-%m-%y')

#Function to call the main url to gadher the data source urls
def fetchurls(**kwargs):
	url='https://healthdata.gov/data.json'
	try:
		r=requests.get(url)
		data=r.json()
	except requests.exceptions.RequestException as e:
		print(e)

	dataset=data['dataset']
	for i in range(len(dataset)):
		for val in dataset[i]['keyword']:
			if 'covid-19'== val.lower() or 'coronavirus'== val.lower():
				# print(val.lower())
				link=dataset[i]['distribution'][0]['downloadURL']
				#list_covid_links.append(link)
				name=dataset[i]['title']
				name=name.replace('/','-')
				#list_file_names.append(name)
				t=(link,name)
				list_file_names.append(t)
				#print("link: ", link)
				break
	return list_file_names

#Function to download the csv files into local space
def download_covid_data(path):
	url1,filename=path
	filename = filename.replace(" ","").replace("-","").replace(",","").replace("(","").replace(")","").replace("'","").replace("'","")
	filename=filename+'.csv'
	print("filename: ", filename)
	print("dirname: ", local_dir_path)
	if not os.path.exists(local_dir_path+filename):
		r=requests.get(url1)
		if r.status_code==200:
			with open(os.path.join(local_dir_path,filename),'wb') as f:
				f.write(r.content)

#Function to fetch all the data source urls from XCOM
def fetch_data_to_local(**kwargs):
	ti = kwargs['ti']
	v1 = ti.xcom_pull(key=None, task_ids='fetch_url')
	print("---------------", v1)
	#results=ThreadPool(8).imap_unordered(download_covid_data,v1)
	for val in v1:
		#x,y=val
		#y = y.replace(" ","")
		download_covid_data(val)

#Function to load the table
def load_covid_diagnostic_lab_testing():
	pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()
	
	def get_pg_table(sql_query, database = connection):
		table = pd.read_sql_query(sql_query, database)
		return table

	df_states = get_pg_table("select * from states_r")
	df_calendar = get_pg_table("select * from calendar_r")

	#Change the select query where clause as per the table load
	df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_diagnostic_lab_testing'")

	#Change the path of the source file as per the table load
	df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19DiagnosticLaboratoryTestingPCRTestingTimeSeries.csv')
	pickup_value = df_pickupval['pickup_criteria'].tolist()
	df_incr_list=[]

	#Change the calendar column name as per the table load
	df_loadcsv_list= df_loadcsv['date'].tolist()
	for val in df_loadcsv_list:
		ax=datetime.strptime(val,'%Y-%m-%d').date()
		df_incr_list.append(ax)

	#Change the calendar column name as per the table load
	df_loadcsv['date'] = df_incr_list
	df_loadcsv = df_loadcsv[df_loadcsv['date']>pickup_value[0]]

	#Change the column names as per the table load 
	df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
	df_join_states['date_format'] = pd.to_datetime(df_join_states.date)
	df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
	df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
	df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

	#Change with the required column names
	df_drop_cols = df_final_join[['state_key', 'calendar_key', 'state_fips', 'fema_region', 'overall_outcome', 'new_results_reported', 'total_results_reported','created_datetime']]

	#Change with the required column names
	for i, row in df_drop_cols.iterrows():
		sql = "INSERT INTO public.covid_diagnostic_lab_testing (state_key, calendar_key, state_fips, fema_region, overall_outcome, new_results_reported, total_results_reported, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s)"
		cursor.execute(sql, tuple(row))

	#Change the update query where clause as per the table load
	sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_diagnostic_lab_testing'  "
	if df_final_join['date'].count() > 0 :
		cursor.execute(sql_upd, (df_final_join['date'].max() , df_final_join['date'].max()) )

	connection.commit()
	connection.close()
	

#Function to load the table
def load_covid_policy_orders():
	pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()
	
	def get_pg_table(sql_query, database = connection):
		table = pd.read_sql_query(sql_query, database)
		return table

	df_states = get_pg_table("select * from states_r")
	df_calendar = get_pg_table("select * from calendar_r")

	#Change the select query where clause as per the table load
	df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_policy_orders'")

	#Change the path of the source file as per the table load
	df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19StateandCountyPolicyOrders.csv')
	pickup_value = df_pickupval['pickup_criteria'].tolist()
	df_incr_list=[]

	#Change the calendar column name as per the table load
	df_loadcsv_list= df_loadcsv['date'].tolist()
	for val in df_loadcsv_list:
		ax=datetime.strptime(val,'%Y-%m-%d').date()
		df_incr_list.append(ax)

	#Change the calendar column name as per the table load
	df_loadcsv['date'] = df_incr_list
	df_loadcsv = df_loadcsv[df_loadcsv['date']>pickup_value[0]]

	#Change the column names as per the table load 
	df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state_id')
	df_join_states['date_format'] = pd.to_datetime(df_join_states.date)
	
	df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
	df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
	df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

	#Change with the required column names
	df_drop_cols = df_final_join[['state_key', 'calendar_key', 'county', 'fips_code', 'policy_level', 'policy_type', 'start_stop', 'comments', 'source', 'total_phases', 'created_datetime']]

	#Change with the required column names
	for i, row in df_drop_cols.iterrows():
		sql = """INSERT INTO public.covid_policy_orders
(state_key, calendar_key, county, fips_code, policy_level, policy_type, start_stop, "comments", "source", total_phases, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
		cursor.execute(sql, tuple(row))

	#Change the update query where clause as per the table load
	sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_policy_orders' "
	if df_final_join['date'].count() > 0 :
		cursor.execute(sql_upd, (df_final_join['date'].max() , df_final_join['date'].max()) )

	connection.commit()
	connection.close()


#Function to load the table
def load_Provisional_Covid_Deaths_by_County():
	pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()
	
	def get_pg_table(sql_query, database = connection):
		table = pd.read_sql_query(sql_query, database)
		return table

	df_states = get_pg_table("select * from states_r")
	df_calendar = get_pg_table("select * from calendar_r")

	#Change the select query where clause as per the table load
	df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'provisional_covid_deaths_by_county'")

	#Change the path of the source file as per the table load
	df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHCountylevelProvisionalCOVID19DeathsCounts.csv')
	pickup_value = df_pickupval['pickup_criteria'].tolist()
	df_incr_list=[]

	#Change the calendar column name as per the table load
	df_loadcsv_list= df_loadcsv['EndWeek'].tolist()
	for val in df_loadcsv_list:
		ax=datetime.strptime(val,'%Y-%m-%d').date()
		df_incr_list.append(ax)

	#Change the calendar column name as per the table load
	df_loadcsv['EndWeek'] = df_incr_list
	df_loadcsv = df_loadcsv[df_loadcsv['EndWeek']>pickup_value[0]]

	#Change the column names as per the table load 
	df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
	df_join_states['endweek_date_format'] = pd.to_datetime(df_join_states.EndWeek)
	
	df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
	df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='endweek_date_format', right_on='date_format')
	df_final_join['startweek_date_format'] = pd.to_datetime(df_final_join.StartWeek)
	df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='startweek_date_format', right_on='date_format')
	df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

	#Change with the required column names
	df_drop_cols = df_final_join[['calendar_key_y', 'calendar_key_x', 'state_key', 'County', 'Fips Code', 'COVID-19 Deaths', 'Total Deaths', 'created_datetime']]

	#Change with the required column names
	for i, row in df_drop_cols.iterrows():
		sql = """INSERT INTO public.provisional_covid_deaths_by_county
(week_start_calendar_key, week_end_calendar_key, state_key, county, fips_code, covid_19_deaths, total_deaths, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s)"""
		cursor.execute(sql, tuple(row))

	#Change the update query where clause as per the table load
	sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_covid_deaths_by_county' "
	if df_final_join['EndWeek'].count() > 0 :
		cursor.execute(sql_upd, (df_final_join['EndWeek'].max() , df_final_join['EndWeek'].max()) )

	connection.commit()
	connection.close()
















	
def delete_localfile_covid_diagnostic_lab_testing():
	os.remove('/usr/local/airflow/covid_data/COVID19DiagnosticLaboratoryTestingPCRTestingTimeSeries.csv')

def delete_localfile_covid_policy_orders():
	os.remove('/usr/local/airflow/covid_data/COVID19StateandCountyPolicyOrders.csv')
	
def delete_Provisional_Covid_Deaths_by_County():
	os.remove('/usr/local/airflow/covid_data/AHCountylevelProvisionalCOVID19DeathsCounts.csv')
	
default_args = { "owner": "airflow", "start_date": datetime.today() - timedelta(days=1) }

dag = DAG( 'covid_data_dag1', default_args=default_args,schedule_interval = "0 6 * * *" )

fetchurls = PythonOperator( task_id='fetch_url', python_callable=fetchurls, dag = dag, provide_context=True)

fetch_data_to_local = PythonOperator( task_id='fetch_data', python_callable=fetch_data_to_local, dag = dag , provide_context=True)

covid_diagnostic_lab_testing = PythonOperator( task_id='load_covid_diagnostic_lab_testing', python_callable=load_covid_diagnostic_lab_testing, dag = dag )

delete_localfile_covid_diagnostic_lab_testing = PythonOperator( task_id='delete_localfile_covid_diagnostic_lab_testing', python_callable=delete_localfile_covid_diagnostic_lab_testing, dag = dag )

covid_policy_orders = PythonOperator( task_id='load_covid_policy_orders', python_callable=load_covid_policy_orders, dag = dag )

delete_localfile_covid_policy_orders = PythonOperator( task_id='delete_localfile_covid_policy_orders', python_callable=delete_localfile_covid_policy_orders, dag = dag )

Provisional_Covid_Deaths_by_County = PythonOperator( task_id='load_Provisional_Covid_Deaths_by_County', python_callable=load_Provisional_Covid_Deaths_by_County, dag = dag )

delete_Provisional_Covid_Deaths_by_County = PythonOperator( task_id='delete_Provisional_Covid_Deaths_by_County', python_callable=delete_Provisional_Covid_Deaths_by_County, dag = dag )

#Setting dependencies between the tasks
fetchurls >> fetch_data_to_local 
fetch_data_to_local >> [ covid_diagnostic_lab_testing , covid_policy_orders , Provisional_Covid_Deaths_by_County ]
covid_diagnostic_lab_testing >> delete_localfile_covid_diagnostic_lab_testing
covid_policy_orders >> delete_localfile_covid_policy_orders
Provisional_Covid_Deaths_by_County >> delete_Provisional_Covid_Deaths_by_County


