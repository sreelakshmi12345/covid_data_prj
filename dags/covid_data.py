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
	filename = filename.replace(" ","").replace("-","").replace(",","").replace("(","").replace(")","").replace("'","").replace("'","").replace(".","").replace("–","").replace(":","")
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
		download_covid_data(val)

#src filename:AH County-level Provisional COVID-19 Deaths Counts.csv
#target table name: Provisional_Covid_Deaths_by_County
def load_Provisional_Covid_Deaths_by_County():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
				table = pd.read_sql_query(sql_query, database)
				return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")

		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'provisional_covid_deaths_by_county'")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHCountylevelProvisionalCOVID19DeathsCounts.csv')
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_StartWeek=[]
		df_StartWeek=df_loadcsv['StartWeek'].tolist()
		for x in df_StartWeek:
				date_formatted=datetime.strptime(x,'%d/%m/%Y').date()
				df_list_StartWeek.append(date_formatted)
		df_loadcsv['StartWeek']=df_list_StartWeek
		df_list_EndWeek=[]
		df_EndWeek=df_loadcsv['EndWeek'].tolist()
		for x in df_EndWeek:
				date_formatted=datetime.strptime(x,'%d/%m/%Y').date()
				df_list_EndWeek.append(date_formatted)
		df_loadcsv['EndWeek']=df_list_EndWeek
		df_loadcsv = df_loadcsv[df_loadcsv['EndWeek']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states.EndWeek)
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states.StartWeek)
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','County','Fips Code','COVID-19 Deaths','Total Deaths','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
				#print(row)
				sql = """INSERT INTO Provisional_Covid_Deaths_by_County(week_start_calendar_key,
				week_end_calendar_key,state_key,County,Fips_Code,COVID_19_Deaths,Total_Deaths,Created_Datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
				cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_covid_deaths_by_county' "
		if df_final_join['EndWeek'].count() > 0 :
				cursor.execute(sql_upd, (df_final_join['EndWeek'].max() , df_final_join['EndWeek'].max()) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: provisional_diabetes_deaths
def load_Provisional_Diabetes_Deaths():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHProvisionalDiabetesDeathCounts2020.csv')
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'provisional_diabetes_deaths'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_incr_list=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_loadcsv_list= df_loadcsv['Data as of'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%m/%d/%Y').date()
			df_incr_list.append(ax)
		df_loadcsv['Data as of'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_loadcsv['date_format'] = pd.to_datetime(df_loadcsv['Data as of'])
		
		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='date_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key','Date_Of_Death_Year','Date_Of_Death_Month','AgeGroup','Sex','COVID19','Diabetes.uc','Diabetes.mc','C19PlusDiabetes','C19PlusHypertensiveDiseases','C19PlusMajorCardiovascularDiseases','C19PlusHypertensiveDiseasesAndMCVD','C19PlusChronicLowerRespiratoryDisease','C19PlusKidneyDisease','C19PlusChronicLiverDiseaseAndCirrhosis','C19PlusObesity', 'created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.provisional_diabetes_deaths
	(calendar_key, date_of_death_year, date_of_death_month, agegroup, sex, covid19, diabetes_uc, diabetes_mc, c19plusdiabetes, c19plushypertensivediseases, c19plusmajorcardiovasculardiseases, c19plushypertensivediseasesandmcvd, c19pluschroniclowerrespiratorydisease, c19pluskidneydisease, c19pluschronicliverdiseaseandcirrhosis, c19plusobesity, created_datetime)
	 VALUES(%s,%s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_diabetes_deaths' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , df_final_join['Data as of'].max()) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: sickle_cell_provisional_deaths
def load_Sickle_Cell_Provisional_Deaths():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHSickleCellDiseaseProvisionalDeathCounts20192020.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'sickle_cell_provisional_deaths'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_incr_list=[]

		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_loadcsv_list= df_loadcsv['Data as of'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%m/%d/%Y').date()
			df_incr_list.append(ax)

		df_loadcsv['Data as of'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_loadcsv['date_format'] = pd.to_datetime(df_loadcsv['Data as of'])
		
		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='date_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key','Date of Death Year','Quarter','Race or Hispanic Origin','Age Group','SCD_Underlying','SCD_Multi','SCD and COVID-19','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.sickle_cell_provisional_deaths
	(calendar_key, date_of_death_year, quarter, race_or_hispanic_origin, age_group, scd_underlying, scd_multi, scd_and_covid_19, created_datetime)
	 VALUES(%s,%s, %s, %s,%s, %s, %s, %s, %s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='sickle_cell_provisional_deaths' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , df_final_join['Data as of'].max()) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()


#Function to load the data from file to a table: reimbursement_to_health_care_providers
def load_reimbursement_to_health_care_providers():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		cursor.execute(""" truncate table reimbursement_to_health_care_providers """)
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ClaimsReimbursementtoHealthCareProvidersandFacilitiesforTestingandTreatmentoftheUninsured.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['Provider Name','state_key','City','Claims Paid for Testing','Claims Paid for Treatment','Georeferenced Column','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.reimbursement_to_health_care_providers
	(provider_name, state_key, city, claims_paid_for_testing, claims_paid_for_treatment, georeferenced_column, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_condition_by_agegroup_by_state
def load_covid_deaths_by_condition_by_agegroup_by_state():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/Conditionscontributingtodeathsinvolvingcoronavirusdisease2019COVID19byagegroupandstateUnitedStates.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_condition_by_agegroup_by_state'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start Week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Condition Group','Condition','ICD10_codes','Age Group','Number of COVID-19 Deaths','Flag','created_datetime']]
		df_drop_cols[['Number of COVID-19 Deaths']] = df_drop_cols[['Number of COVID-19 Deaths']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_condition_by_agegroup_by_state
	(week_start_calendar_key, week_end_calendar_key, state_key, date_as_of, condition_group, "condition", icd10_codes, age_group, number_of_covid_19_deaths, flag, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_condition_by_agegroup_by_state' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_case_surveillance_data
def load_covid_case_surveillance_data():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19CaseSurveillancePublicUseData.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_case_surveillance_data'")

		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_cdc_dt=[]
		df_cdc_dt=df_loadcsv['cdc_report_dt'].tolist()
		for x in df_cdc_dt:
			date_formatted=datetime.strptime(x,'%Y/%m/%d').date()
			df_list_cdc_dt.append(date_formatted)
		df_loadcsv['cdc_report_dt']=df_list_cdc_dt
		df_loadcsv = df_loadcsv[df_loadcsv['cdc_report_dt']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_loadcsv['cdc_report_dt_format'] = pd.to_datetime(df_loadcsv.cdc_report_dt)
		df_loadcsv['pos_spec_dt_format'] = pd.to_datetime(df_loadcsv.pos_spec_dt)
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='cdc_report_dt_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='pos_spec_dt_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','onset_dt','current_status','sex','age_group','Race and ethnicity (combined)','hosp_yn','icu_yn','death_yn','medcond_yn','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables		
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_case_surveillance_data
	(cdc_report_key, pos_spec_key, onset_dt, current_status, sex, age_group, race_and_ethnicity, hosp_yn, icu_yn, death_yn, medcond_yn, created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_case_surveillance_data' "
		if df_final_join['cdc_report_dt'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['cdc_report_dt'].max() , pd.to_datetime('today').strftime("%Y-%m-%d") ) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_impact_on_hospital_capacity
def load_covid_impact_on_hospital_capacity():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19EstimatedPatientImpactandHospitalCapacitybyState.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_impact_on_hospital_capacity'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_incr_list=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_loadcsv_list= df_loadcsv['collection_date'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%Y-%m-%d').date()
			df_incr_list.append(ax)
		df_loadcsv['collection_date'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['collection_date']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
		df_join_states['date_format'] = pd.to_datetime(df_join_states.collection_date)
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key','calendar_key','Inpatient Beds Occupied Estimated','Count LL','Count UL','Percentage of Inpatient Beds Occupied Estimated','Percentage LL','Percentage UL','Total Inpatient Beds','Total LL','Total UL','created_datetime']]
		df_drop_cols = df_drop_cols.apply(lambda x: x.replace(',',''))
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = "INSERT INTO public.covid_impact_on_hospital_capacity(state_key, calendar_key, inpatient_beds_occupied_estimated, count_ll, count_ul, percentage_of_inpatient_beds_occupied_estimated, percentage_ll, percentage_ul, total_inpatient_beds, total_ll, total_ul, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s)"
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_diagnostic_lab_testing'  "
		if df_final_join['collection_date'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['collection_date'].max() , pd.to_datetime('today').strftime("%Y-%m-%d") ) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_policy_orders
def load_covid_policy_orders():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19StateandCountyPolicyOrders.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_policy_orders'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_incr_list=[]

		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_loadcsv_list= df_loadcsv['date'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%Y-%m-%d').date()
			df_incr_list.append(ax)

		df_loadcsv['date'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['date']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state_id')
		df_join_states['date_format'] = pd.to_datetime(df_join_states.date)
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key', 'calendar_key', 'county', 'fips_code', 'policy_level', 'policy_type', 'start_stop', 'comments', 'source', 'total_phases', 'created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.covid_policy_orders
	(state_key, calendar_key, county, fips_code, policy_level, policy_type, start_stop, "comments", "source", total_phases, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_policy_orders' "
		if df_final_join['date'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['date'].max() , df_final_join['date'].max()) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_diagnostic_lab_testing
def load_covid_diagnostic_lab_testing():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
		
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19DiagnosticLaboratoryTestingPCRTestingTimeSeries.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_diagnostic_lab_testing'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_incr_list=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_loadcsv_list= df_loadcsv['date'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%Y-%m-%d').date()
			df_incr_list.append(ax)
			
		df_loadcsv['date'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['date']>pickup_value[0]]
		
		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
		df_join_states['date_format'] = pd.to_datetime(df_join_states.date)
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key', 'calendar_key', 'state_fips', 'fema_region', 'overall_outcome', 'new_results_reported', 'total_results_reported','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = "INSERT INTO public.covid_diagnostic_lab_testing (state_key, calendar_key, state_fips, fema_region, overall_outcome, new_results_reported, total_results_reported, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s)"
			cursor.execute(sql, tuple(row))
			
		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_diagnostic_lab_testing'  "
		if df_final_join['date'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['date'].max() , df_final_join['date'].max()) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()
		
#Function to load the data from file to a table: covid_impact_on_hospital_capacity_reported
def load_covid_impact_on_hospital_capacity_reported():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table covid_impact_on_hospital_capacity_reported """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19ReportedPatientImpactandHospitalCapacitybyState.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key','hospital_onset_covid','hospital_onset_covid_coverage','inpatient_beds','inpatient_beds_coverage','inpatient_beds_used','inpatient_beds_used_coverage','inpatient_beds_used_covid','inpatient_beds_used_covid_coverage','staffed_adult_icu_bed_occupancy','staffed_adult_icu_bed_occupancy_coverage','staffed_icu_adult_patients_confirmed_and_suspected_covid','staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage','staffed_icu_adult_patients_confirmed_covid','staffed_icu_adult_patients_confirmed_covid_coverage','total_adult_patients_hospitalized_confirmed_and_suspected_covid','total_adult_patients_hospitalized_confirmed_covid','total_adult_patients_hospitalized_confirmed_covid_coverage','total_pediatric_patients_hospitalized_confirmed_and_suspected_covid','total_pediatric_patients_hospitalized_confirmed_covid','total_pediatric_patients_hospitalized_confirmed_covid_coverage','total_staffed_adult_icu_beds','total_staffed_adult_icu_beds_coverage','inpatient_beds_utilization','inpatient_beds_utilization_coverage','inpatient_beds_utilization_numerator','inpatient_beds_utilization_denominator','percent_of_inpatients_with_covid','percent_of_inpatients_with_covid_coverage','percent_of_inpatients_with_covid_numerator','percent_of_inpatients_with_covid_denominator','inpatient_bed_covid_utilization','inpatient_bed_covid_utilization_coverage','inpatient_bed_covid_utilization_numerator','inpatient_bed_covid_utilization_denominator','adult_icu_bed_covid_utilization','adult_icu_bed_covid_utilization_coverage','adult_icu_bed_covid_utilization_numerator','adult_icu_bed_covid_utilization_denominator','adult_icu_bed_utilization','adult_icu_bed_utilization_coverage','adult_icu_bed_utilization_numerator','adult_icu_bed_utilization_denominator','reporting_cutoff_start','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.covid_impact_on_hospital_capacity_reported(state_key, hospital_onset_covid, hospital_onset_covid_coverage, inpatient_beds, inpatient_beds_coverage, inpatient_beds_used, inpatient_beds_used_coverage, inpatient_beds_used_covid, inpatient_beds_used_covid_coverage, staffed_adult_icu_bed_occupancy, staffed_adult_icu_bed_occupancy_coverage, staffed_icu_adult_patients_confirmed_and_suspected_covid, staffed_icu_adult_patients_confirmed_and_suspected_covid_covera, staffed_icu_adult_patients_confirmed_covid, staffed_icu_adult_patients_confirmed_covid_coverage, total_adult_patients_hospitalized_confirmed_and_suspected_covid, total_adult_patients_hospitalized_confirmed_covid, total_adult_patients_hospitalized_confirmed_covid_coverage, total_pediatric_patients_hospitalized_confirmed_and_suspected_c, total_pediatric_patients_hospitalized_confirmed_covid, total_pediatric_patients_hospitalized_confirmed_covid_coverage, total_staffed_adult_icu_beds, total_staffed_adult_icu_beds_coverage, inpatient_beds_utilization, inpatient_beds_utilization_coverage, inpatient_beds_utilization_numerator, inpatient_beds_utilization_denominator, percent_of_inpatients_with_covid, percent_of_inpatients_with_covid_coverage, percent_of_inpatients_with_covid_numerator, percent_of_inpatients_with_covid_denominator, inpatient_bed_covid_utilization, inpatient_bed_covid_utilization_coverage, inpatient_bed_covid_utilization_numerator, inpatient_bed_covid_utilization_denominator, adult_icu_bed_covid_utilization, adult_icu_bed_covid_utilization_coverage, adult_icu_bed_covid_utilization_numerator, adult_icu_bed_covid_utilization_denominator, adult_icu_bed_utilization, adult_icu_bed_utilization_coverage, adult_icu_bed_utilization_numerator, adult_icu_bed_utilization_denominator, reporting_cutoff_start, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age
def load_cumulative_provisional_countsof_deathsby_sex_race_age():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
		
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/CumulativeProvisionalCountsofDeathsbySexRaceandAge.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'cumulative_provisional_countsof_deathsby_sex_race_age'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_incr_list=[]
		df_loadcsv_list= df_loadcsv['Data as of'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%m/%d/%Y').date()
			df_incr_list.append(ax)
			
		df_loadcsv['Data as of'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]
		print(df_loadcsv)
		print('________________oickval______________________________',pickup_value[0])
		#Joining the source data with the reference tables to populate keys
		df_loadcsv['Analysis Period Start Date'] = pd.to_datetime(df_loadcsv['Analysis Period Start Date'])
		df_loadcsv['Analysis Period End Date']=pd.to_datetime(df_loadcsv['Analysis Period End Date'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='Analysis Period Start Date', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='Analysis Period End Date', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		
		df_drop_cols = df_final_join[['calendar_key_y', 'calendar_key_y','Data as of','Sex','Race/Ethnicity','Age group','Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)','created_datetime']]
		df_drop_cols[['Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)']]=df_drop_cols[['Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		#rows_count=df_drop_cols.count()
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age (week_start_calendar_key, week_end_calendar_key, data_as_of, sex, race_ethnicity, age_group, "Year", mmwrweek, allcause, naturalcause, septicemia, malignant_neoplasms, diabetes_mellitus, alzheimer_disease, influenza_and_pneumonia, chronic_lower_respiratory_diseases, other_diseases_of_respiratory_system, nephritis__nephrotic_syndrome_and_nephrosis, symptoms_signs_and_abnormal_clinical_and_laboratory_findings_no, diseases_of_heart, cerebrovascular_diseases, covid_19_u071_multiple_cause_of_death, covid_19_u071_underlying_cause_of_death, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s)"""
			cursor.execute(sql, tuple(row))
			
		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='cumulative_provisional_countsof_deathsby_sex_race_age'  "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age_7_4_2020
def load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
		
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/CumulativeProvisionalDeathCountsbySexRaceandAgethrough742020.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_incr_list=[]
		df_loadcsv_list= df_loadcsv['Data as of'].tolist()
		for val in df_loadcsv_list:
			ax=datetime.strptime(val,'%m/%d/%Y').date()
			df_incr_list.append(ax)
			
		df_loadcsv['Data as of'] = df_incr_list
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]
		print(df_loadcsv)
		print('________________oickval______________________________',pickup_value[0])
		#Joining the source data with the reference tables to populate keys
		df_loadcsv['Analysis Period Start Date'] = pd.to_datetime(df_loadcsv['Analysis Period Start Date'])
		df_loadcsv['Analysis Period End Date']=pd.to_datetime(df_loadcsv['Analysis Period End Date'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='Analysis Period Start Date', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='Analysis Period End Date', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		
		df_drop_cols = df_final_join[['calendar_key_y', 'calendar_key_y','Data as of','Sex','Race/Ethnicity','Age group','Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)','created_datetime']]
		df_drop_cols[['Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)']]=df_drop_cols[['Year','MMWRWeek','AllCause','NaturalCause','Septicemia (A40-A41)','Malignant neoplasms (C00-C97)','Diabetes mellitus (E10-E14)','Alzheimer disease (G30)','Influenza and pneumonia (J09-J18)','Chronic lower respiratory diseases (J40-J47)','Other diseases of respiratory system (J00-J06,J30-J39,J67,J70-J98)','Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)','Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified (R00-R99)','Diseases of heart (I00-I09,I11,I13,I20-I51)','Cerebrovascular diseases (I60-I69)','COVID-19 (U071, Multiple Cause of Death)','COVID-19 (U071, Underlying Cause of Death)']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		#rows_count=df_drop_cols.count()
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020 (week_start_calendar_key, week_end_calendar_key, data_as_of, sex, race_ethnicity, age_group, "Year", mmwrweek, allcause, naturalcause, septicemia, malignant_neoplasms, diabetes_mellitus, alzheimer_disease, influenza_and_pneumonia, chronic_lower_respiratory_diseases, other_diseases_of_respiratory_system, nephritis__nephrotic_syndrome_and_nephrosis, symptoms_signs_and_abnormal_clinical_and_laboratory_findings_no, diseases_of_heart, cerebrovascular_diseases, covid_19_u071_multiple_cause_of_death, covid_19_u071_underlying_cause_of_death, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s)"""
			cursor.execute(sql, tuple(row))
			
		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020'  "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: deaths_involving_covid_by_race_hispanic_originggroup_age_by_state
def load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/Deathsinvolvingcoronavirusdisease2019COVID19byraceandHispanicorigingroupandagebystate.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'deaths_involving_covid_by_race_hispanic_group_age_by_state'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x', 'state_key', 'Data as of', 'Age group','Race and Hispanic Origin Group','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
		df_drop_cols[['COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths']] = df_drop_cols[['COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.deaths_involving_covid_by_race_hispanic_group_age_by_state
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, age_group, race_and_hispanic_origin_group, covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='deaths_involving_covid_by_race_hispanic_group_age_by_state' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: distribution_of_covid_deaths_by_juristriction_by_age
def load_distribution_of_covid_deaths_by_juristriction_by_age():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/DistributionofCOVID19deathsandpopulationsbyjurisdictionageandraceandHispanicorigin.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'distribution_of_covid_deaths_by_juristriction_by_age'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x', 'state_key', 'Data as of','Race/Hispanic origin','Count of COVID-19 deaths','Distribution of COVID-19 deaths (%)','Unweighted distribution of population (%)','Weighted distribution of population (%)','Difference between COVID-19 and unweighted population %','Difference between COVID-19 and weighted population %','AgeGroup','Suppression', 'created_datetime']]
		df_drop_cols[['Count of COVID-19 deaths','Distribution of COVID-19 deaths (%)','Unweighted distribution of population (%)','Weighted distribution of population (%)','Difference between COVID-19 and unweighted population %','Difference between COVID-19 and weighted population %']] = df_drop_cols[['Count of COVID-19 deaths','Distribution of COVID-19 deaths (%)','Unweighted distribution of population (%)','Weighted distribution of population (%)','Difference between COVID-19 and unweighted population %','Difference between COVID-19 and weighted population %']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.distribution_of_covid_deaths_by_juristriction_by_age
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, race_hispanic_origin, count_of_covid19_deaths, distribution_of_covid19_deaths, unweighted_distribution_of_population, weighted_distribution_of_population, difference_between_covid19_and_unweighted_population, difference_between_covid19_and_weighted_population, agegroup, suppression, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='distribution_of_covid_deaths_by_juristriction_by_age' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#function to load table excess_deaths_associated_with_covid
def load_excess_deaths_associated_with_covid():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
				table = pd.read_sql_query(sql_query, database)
				return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")

		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'excess_deaths_associated_with_covid'")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ExcessDeathsAssociatedwithCOVID19.csv')
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_StartWeek=[]
		df_StartWeek=df_loadcsv['Week Ending Date'].tolist()
		for x in df_StartWeek:
				date_formatted=datetime.strptime(x,'%Y-%m-%d').date()
				df_list_StartWeek.append(date_formatted)
		df_loadcsv['Week Ending Date']=df_list_StartWeek
		df_loadcsv = df_loadcsv[df_loadcsv['Week Ending Date']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['Week Ending Date'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key','state_key','Observed Number','Upper Bound Threshold','Exceeds Threshold','Average Expected Count','Excess Lower Estimate','Excess Higher Estimate','Year','Total Excess Lower Estimate in 2020','Total Excess Higher Estimate in 2020','Percent Excess Lower Estimate','Percent Excess Higher Estimate','Type','Outcome','Suppress','Note','created_datetime']]
		df_drop_cols[[ 'Average Expected Count','Excess Lower Estimate','Excess Higher Estimate','Total Excess Lower Estimate in 2020','Total Excess Higher Estimate in 2020','Percent Excess Lower Estimate','Percent Excess Higher Estimate' ,'Observed Number','Upper Bound Threshold']] = df_drop_cols[[ 'Average Expected Count','Excess Lower Estimate','Excess Higher Estimate','Total Excess Lower Estimate in 2020','Total Excess Higher Estimate in 2020','Percent Excess Lower Estimate','Percent Excess Higher Estimate','Observed Number','Upper Bound Threshold' ]].fillna(value = 0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
				#print(row)
				sql = """INSERT INTO excess_deaths_associated_with_covid(week_end_calendar_key, state_key, observed_number, upper_bound_threshold, exceeds_threshold, average_expected_count, excess_lower_estimate, excess_higher_estimate, "Year", total_excess_lower_estimate_in_2020, total_excess_higher_estimate_in_2020, percent_excess_lower_estimate, percent_excess_higher_estimate, "Type", outcome, suppress, note, created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
				cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='excess_deaths_associated_with_covid' "
		if df_final_join['Week Ending Date'].count() > 0 :
				cursor.execute(sql_upd, (df_final_join['Week Ending Date'].max() , pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()



#Function to load the data from file to a table: load_indicators_based_on_reported_freq_symptoms
def load_indicators_based_on_reported_freq_symptoms():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table indicators_based_on_reported_freq_symptoms """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/IndicatorsofAnxietyorDepressionBasedonReportedFrequencyofSymptomsDuringLast7Days.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key', 'Phase','Indicator','Group','Subgroup','Time Period','Time Period Label','Value','Low CI','High CI','Confidence Interval','Quartile range','created_datetime']]
		df_drop_cols[[ 'Value','Low CI','High CI'  ]] = df_drop_cols[[ 'Value','Low CI','High CI'  ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.indicators_based_on_reported_freq_symptoms
	(state_key, phase, "Indicator", "Group", subgroup, time_period, time_period_label, value, low_ci, high_ci, confidence_interval, quartile_range, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()


#Function to load the data from file to a table: indicators_of_health_insurance_coverage
def load_indicators_of_health_insurance_coverage():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table indicators_of_health_insurance_coverage """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/IndicatorsofHealthInsuranceCoverageattheTimeofInterview.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

		#Change with the required column names
		df_drop_cols = df_final_join[['state_key', 'Phase','Indicator','Group','Subgroup','Time Period','Time Period Label','Value','Low CI','High CI','Confidence Interval','Quartile Range','Suppression Flag','created_datetime']]
		df_drop_cols[[ 'Value','Low CI','High CI','Suppression Flag'  ]] = df_drop_cols[[ 'Value','Low CI','High CI', 'Suppression Flag'  ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.indicators_of_health_insurance_coverage
	(state_key, phase, "Indicator", "Group",  subgroup, time_period, time_period_label, value, low_ci, high_ci, confidence_interval, quartile_range, supression_flag, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: indicators_reduced_access_care_dueto_covid
def load_indicators_reduced_access_care_dueto_covid():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table indicators_reduced_access_care_dueto_covid """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/IndicatorsofReducedAccesstoCareDuetotheCoronavirusPandemicDuringLast4Weeks.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key', 'Phase','Indicator','Group','Subgroup','Week','Week Label','Value','Low CI','High CI','Confidence Interval','Quartile Range','created_datetime']]
		df_drop_cols[[ 'Value','Low CI','High CI'  ]] = df_drop_cols[[ 'Value','Low CI','High CI'  ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.indicators_reduced_access_care_dueto_covid
	(state_key, phase, "Indicator", "Group",  subgroup, "week", "week_label", value, low_ci, high_ci, confidence_interval, quartile_range, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: loss_of_work_due_to_illness
def load_loss_of_work_due_to_illness():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table loss_of_work_due_to_illness """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/LossofWorkDuetoIllnessRANDSduringCOVID19.csv')

		#Joining the source data with the reference tables to populate keys
		df_loadcsv['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_loadcsv[['Round','Indicator','Group','Subgroup','Sample Size','Percent','Standard Error','Suppression','Significant', 'created_datetime']]
		df_drop_cols[[ 'Sample Size','Percent','Standard Error','Suppression','Significant'  ]] = df_drop_cols[[ 'Sample Size','Percent','Standard Error','Suppression','Significant'  ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.loss_of_work_due_to_illness
	("Round", "Indicator", "Group", subgroup, sample_size, "percent", standard_error, supression, significant, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: mental_healthcare
def load_mental_healthcare():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()		
		cursor.execute(""" truncate table mental_healthcare """)
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/MentalHealthCareintheLast4Weeks.csv')

		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['state_key', 'Phase','Indicator','Group','Subgroup','Time Period','Time Period Label','Value','LowCI','HighCI','Confidence Interval','Quartile Range','Suppression Flag','created_datetime']]
		df_drop_cols[[ 'Value','LowCI','HighCI' , 'Suppression Flag' ]] = df_drop_cols[[ 'Value','LowCI','HighCI' , 'Suppression Flag' ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			sql = """INSERT INTO public.mental_healthcare
	(state_key, phase, "Indicator", "Group", subgroup, time_period, time_period_label, value, low_ci, high_ci, confidence_interval, quartile_range, supression_flag, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: monthly_covid_deaths_by_reagion_age_race 
def load_monthly_covid_deaths_by_reagion_age_race():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageandraceandHispanicorigin.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'monthly_covid_deaths_by_reagion_age_race'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()

		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]
		df_loadcsv['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

		#Joining the source data with the reference tables to populate
		df_drop_cols = df_loadcsv[['Data as of','Date Of Death Year','Date Of Death Month','Region','AgeGroup','RaceEthnicity','COVID-19 (U071, Multiple Cause of Death)','Sex','Place Of Death','Note1','Note2','created_datetime']]
		df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'] = df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.monthly_covid_deaths_by_reagion_age_race
	(data_as_of, date_of_death_year, date_of_death_month, region, agegroup, raceethnicity, covid_19_u071_multiple_cause_of_death, sex, place_of_death, note1, note2, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='monthly_covid_deaths_by_reagion_age_race' "
		if df_drop_cols['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_drop_cols['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: monthly_covid_deaths_by_region_age_race_place
def load_monthly_covid_deaths_by_region_age_race_place():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageplaceandraceandHispanicorigin.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'monthly_covid_deaths_by_region_age_race_place'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['AnalysisDate'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%Y-%m-%d').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['AnalysisDate']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['AnalysisDate']>pickup_value[0]]
		df_loadcsv['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

		#Joining the source data with the reference tables to populate keys
		df_drop_cols = df_loadcsv[['AnalysisDate','Date Of Death Year','Date Of Death Month','Region','AgeGroup','RaceEthnicity','COVID-19 (U071, Multiple Cause of Death)','Sex','Place Of Death','Note','flag_cov19mcod','created_datetime']]
		df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'] = df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.monthly_covid_deaths_by_region_age_race_place
	(AnalysisDate, date_of_death_year, date_of_death_month, region, agegroup, raceethnicity, covid_19_u071_multiple_cause_of_death, sex, place_of_death, note, flag_cov19mcod, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='monthly_covid_deaths_by_region_age_race_place' "
		if df_drop_cols['AnalysisDate'].count() > 0 :
			cursor.execute(sql_upd, (df_drop_cols['AnalysisDate'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()	

#Function to load the data from file to a table: covid_deaths_by_county_and_race
def load_covid_deaths_by_county_and_race():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyCountyandRace.csv')

		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_county_and_race'")
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','County Name','Urban Rural Code','FIPS State','FIPS County','FIPS Code','Indicator','Total deaths','COVID-19 Deaths','Non-Hispanic White','Non-Hispanic Black','Non-Hispanic American Indian or Alaska Native','Non-Hispanic Asian','Other','Hispanic','Urban Rural Description','Footnote','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_county_and_race
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, county_name, urban_rural_code, fips_state, fips_county, fips_code, "indicator", total_deaths, covid_19_deaths, non_hispanic_white, non_hispanic_black, non_hispanic_american_indian_or_alaska_native, non_hispanic_asian, other, hispanic, urban_rural_description, footnote, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_county_and_race' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_deathplace_by_state 
def load_covid_deaths_by_deathplace_by_state():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyPlaceofDeathandState.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_deathplace_by_state'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Place of Death','COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths','Footnote','created_datetime']]
		df_drop_cols[[ 'COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths' ]] = df_drop_cols[[ 'COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths' ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_deathplace_by_state
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, place_of_death, covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_deathplace_by_state' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_sex_age_by_state
def load_covid_deaths_by_sex_age_by_state():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandState.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_sex_age_by_state'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Sex','Age group','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
		df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]] = df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_sex_age_by_state
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, sex, age_group,covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_sex_age_by_state' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_sex_age_by_week
def load_covid_deaths_by_sex_age_by_week():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandWeek.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_sex_age_by_week'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key','state_key','Data as of','Sex','Age Group','COVID-19 Deaths','Total Deaths','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_sex_age_by_week
	(week_end_calendar_key, state_key, data_as_of, sex, age_group, covid19_deaths, total_deaths, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_sex_age_by_week' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()


#Function to load the data from file to a table: covid_deaths_by_weekend_by_state 
def load_covid_deaths_by_weekend_by_state():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyWeekEndingDateandState.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_weekend_by_state'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Group','Indicator','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
		df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]] = df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]].fillna(0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_weekend_by_state
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, "group", "Indicator",covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_weekend_by_state' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_county
def load_covid_deaths_by_county():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsintheUnitedStatesbyCounty.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_county'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		df_list_DataasOf=[]
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		df_DataasOf=df_loadcsv['Date as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Date as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Date as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['Last week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['First week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Date as of','County name','FIPS County Code','Deaths involving COVID-19','Deaths from All Causes','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_county
	(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, county_name, fips_county, deaths_involving_covid_19, deaths_from_all_causes, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_county' "
		if df_final_join['Date as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Date as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Function to load the data from file to a table: covid_deaths_by_age_in_years
def load_covid_deaths_by_age_in_years():
	try:
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database Connection Established Sucsussfully....................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table

		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathsCountsbyAgeinYears.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_age_in_years'")
		pickup_value = df_pickupval['pickup_criteria'].tolist()

		#Format the date column into required format and read only rows whose date is greater then the value of date in 
		df_list_DataasOf=[]
		df_DataasOf=df_loadcsv['Data as of'].tolist()
		for x in df_DataasOf:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_DataasOf.append(date_formatted)
		df_loadcsv['Data as of']=df_list_DataasOf
		df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_loadcsv['EndWeekdate_format'] = pd.to_datetime(df_loadcsv['End Week'])
		df_loadcsv['StartWeekdate_format'] = pd.to_datetime(df_loadcsv['Start Week'])
		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','Data as of','Sex','Age Years','Total deaths','COVID-19 Deaths','created_datetime']]

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		#Loading the data into the tables
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO public.covid_deaths_by_age_in_years
	(week_start_calendar_key, week_end_calendar_key, data_as_of, sex, age_years, total_deaths, covid19_deaths, created_datetime)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_age_in_years' "
		if df_final_join['Data as of'].count() > 0 :
			cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

	#Raise Exceptions
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()


#Load data from sourse file into a table:Provisional_Deaths_coronavirus
def load_Provisional_Deaths_coronavirus():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalDeathCountsforCoronavirusDiseaseCOVID19:DistributionofDeathsbyRaceandHispanicOrigin..csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'Provisional_Deaths_coronavirus'")
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		
		df_list_EndWeek=[]
		df_EndWeek=df_loadcsv['End week'].tolist()
		for x in df_EndWeek:
			date_formatted=datetime.strptime(x,'%d/%m/%Y').date()
			df_list_EndWeek.append(date_formatted)
		df_loadcsv['End week']=df_list_EndWeek

	
		df_loadcsv = df_loadcsv[df_loadcsv['End week']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End week'])
		df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])

		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		#Change with the required column names
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Group','Indicator','All COVID-19 Deaths (U07.1)','Deaths from All Causes','Percent of Expected Deaths','All Pneumonia Deaths (J12.0-J18.9)','Deaths with Pneumonia and COVID-19 (J12.0-J18.9 and U07.1)','All Influenza Deaths (J09-J11)','Pneumonia, Influenza, and COVID-19 Deaths','Footnote','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO Provisional_Deaths_coronavirus(week_start_calendar_key,week_end_calendar_key,state_key,Data_as_of,group,Indicator,All_COVID_19_Deaths_U07_1,Deaths_from_All_Causes,Percent_of_Expected_Deaths,All_Pneumonia_Deaths_J12_0_J18_9,Deaths_with_Pneumonia_and_COVID_19_J120_J189_and_U071,All_Influenza_Deaths_J09_J11,Pneumonia_Influenza_and_COVID_19_Deaths,Footnote,created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='Provisional_Deaths_coronavirus' "
		if df_final_join['End week'].count() > 0 :
				cursor.execute(sql_upd, (df_final_join['End week'].max() ,pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))
	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()
	
	
#Load data from sourse file into a table:Provisional_Death_Counts_Influenza_Pneumonia_and_COVID
def load_provisional_death_counts_influenza_pneumonia_and_covid():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalDeathCountsforInfluenzaPneumoniaandCOVID19.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'provisional_death_counts_influenza_pneumonia_and_covid'")
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
				
		df_list_EndWeek=[]
		df_EndWeek=df_loadcsv['End Week'].tolist()
		for x in df_EndWeek:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_EndWeek.append(date_formatted)
		df_loadcsv['End Week']=df_list_EndWeek
		
		df_loadcsv = df_loadcsv[df_loadcsv['End Week']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		#df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
		df_loadcsv['EndWeekdate_format'] = pd.to_datetime(df_loadcsv['End Week'])
		df_loadcsv['StartWeekdate_format'] = pd.to_datetime(df_loadcsv['Start Week'])

		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
		df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		#Change with the required column names
		df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','Data As Of','MMWRyear','MMWRweek','Group','Indicator','Jurisdiction','Age Group','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Influenza Deaths','Pneumonia or Influenza','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
		df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Influenza Deaths','Pneumonia or Influenza','Pneumonia, Influenza, or COVID-19 Deaths' ]] = df_drop_cols[['COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Influenza Deaths','Pneumonia or Influenza','Pneumonia, Influenza, or COVID-19 Deaths']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO provisional_death_counts_influenza_pneumonia_and_covid(week_start_calendar_key, week_end_calendar_key, data_as_of, mmwryear, mmwrweek, "Group", "Indicator", jurisdiction, age_group, covid_19_deaths, total_deaths, pneumonia_deaths, influenza_deaths, pneumonia_or_influenza, pneumonia_influenza_or_covid_19_deaths, footnote, created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_death_counts_influenza_pneumonia_and_covid' "
		if df_final_join['End Week'].count() > 0 :
				cursor.execute(sql_upd, (df_final_join['End Week'].max() ,pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))
	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()
		


#Load data from sourse file into a table:Reduced_Access_to_Care_RANDS_during_COVID
def load_Reduced_Access_to_Care_RANDS_during_COVID():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print("Database Connection is Established Sucsussfully...............................")
		cursor.execute("Truncate table Reduced_Access_to_Care_RANDS_during_COVID")
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		#df_states = get_pg_table("select * from states_r")
		#df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ReducedAccesstoCareRANDSduringCOVID19.csv')
		
		df_final_join=df_loadcsv
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		#Take the required columns 
		df_drop_cols = df_final_join[['Round','Indicator','Group','Subgroup','Sample Size','Response','Percent','Standard Error','Suppression','Significant','created_datetime']]
		df_drop_cols[['Round','Sample Size','Percent','Standard Error','Suppression','Significant']]=df_drop_cols[['Round','Sample Size','Percent','Standard Error','Suppression','Significant']].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO Reduced_Access_to_Care_RANDS_during_COVID("Round","Indicator","Group",Subgroup,Sample_Size,Response,"Percent",Standard_Error,Suppression,Significant,created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		#sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_death_counts_influenza_pneumonia_and_covid' "
		#if df_final_join['EndWeek'].count() > 0 :
				#cursor.execute(sql_upd, (df_final_join['End week'].max() ,pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))
	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Load data from sourse file into a table:Telemedicine_RANDS_during_COVID19
def load_Telemedicine_RANDS_during_COVID19():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print("Database Connection is Established Sucsussfully...............................")
		cursor.execute("Truncate table Telemedicine_RANDS_during_COVID19")
	
			
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/TelemedicineRANDSduringCOVID19.csv')
		
		df_final_join=df_loadcsv
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		#Take the required columns 
		df_drop_cols = df_final_join[['Round','Indicator','Group','Subgroup','Sample Size','Response','Percent','Standard Error','Suppression','Significant','created_datetime']]
		df_drop_cols[['Round','Sample Size','Percent','Standard Error','Suppression','Significant']]=df_drop_cols[['Round','Sample Size','Percent','Standard Error','Suppression','Significant']].fillna(value=0)

		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO Telemedicine_RANDS_during_COVID19("Round","Indicator","Group",Subgroup,Sample_Size,Response,"Percent",Standard_Error,Suppression,Significant,created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#load data intoo a table :UnitedStates_COVID19_Cases_Deaths_by_State_overTime
def load_unitedstates_covid19_cases_deaths_by_state_overtime():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_calendar = get_pg_table("select * from calendar_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/UnitedStatesCOVID19CasesandDeathsbyStateoverTime.csv')
		
		#Capturing last processed date to load the data incrementally
		df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'unitedstates_covid19_cases_deaths_by_state_overtime'")
		
		#Format the date column into required format and read only rows whose date is greater then the value of date in audit_table
		pickup_value = df_pickupval['pickup_criteria'].tolist()
		
		df_list_submission_date=[]
		df_submission_date=df_loadcsv['submission_date'].tolist()
		for x in df_submission_date:
			date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
			df_list_submission_date.append(date_formatted)
		df_loadcsv['submission_date']=df_list_submission_date

	
		df_loadcsv = df_loadcsv[df_loadcsv['submission_date']>pickup_value[0]]

		#Joining the source data with the reference tables to populate keys
		df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
		df_join_states['submission_date_format'] = pd.to_datetime(df_join_states['submission_date'])
		

		df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
		df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='submission_date_format', right_on='date_format')
		#df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		#Change with the required column names
		df_drop_cols = df_final_join[['calendar_key','state_key','submission_date','state','tot_cases','conf_cases','prob_cases','new_case','pnew_case','tot_death','conf_death','prob_death','new_death','pnew_death','created_at','consent_cases','consent_deaths','created_datetime']]
		df_drop_cols[['tot_cases','conf_cases','prob_cases','new_case','pnew_case','tot_death','conf_death','prob_death','new_death','pnew_death']]=df_drop_cols[['tot_cases','conf_cases','prob_cases','new_case','pnew_case','tot_death','conf_death','prob_death','new_death','pnew_death']].fillna(value=0)
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO UnitedStates_COVID19_Cases_Deaths_by_State_overTime(state_key, calendar_key, submission_date, state, tot_cases, conf_cases, prob_cases, new_case, pnew_case, tot_death, conf_death, prob_death, new_death, pnew_death, created_at, consent_cases, consent_deaths, created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

		#Update the audit table with the date,for incremnetal batch process.
		sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='unitedstates_covid19_cases_deaths_by_state_overtime' "
		if df_final_join['submission_date'].count() > 0 :
				cursor.execute(sql_upd, (df_final_join['submission_date'].max() ,pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S") ))
	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()

#Load data from sourse file into a table:US_Stateand_Territorial_Stay_AtHome_Order
def load_US_Stateand_Territorial_Stay_AtHome_Order():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		cursor.execute("Truncate table US_Stateand_Territorial_Stay_AtHome_Order")
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/USStateandTerritorialStayAtHomeOrdersMarch15July7byCountybyDay.csv')
		
		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State_Tribe_Territory')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		
		#Take required columns to insert in to a target table
		df_drop_cols = df_final_join[['state_key','County_Name','FIPS_State','FIPS_County','date','Current_order_status','Order_code','Issuing_Jurisdiction','Stay_at_Home_Order_Recommendation','Effective_date','Expiration_date','Effective_NA_Reason','Expiration_NA_Reason','Date_Signed','Express_Preemption','origin_dataset','County','Source_of_Action','URL','Citation','flag','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO US_Stateand_Territorial_Stay_AtHome_Order(state_key, county_name, fips_state, fips_county, "date", current_order_status, order_code, issuing_jurisdiction, stay_at_home_order_recommendation, effective_date, expiration_date, effective_na_reason, expiration_na_reason, date_signed, express_preemption, origin_dataset, county, source_of_action, url, citation, flag, created_datetime)) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()
	

	
#Load data from sourse file into a table:counts_of_deaths_by_jurisdiction_and_race_and_Hispanic
def load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		cursor.execute("Truncate table deaths_by_jurisdiction_race_hispanic_weekly")
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/WeeklycountsofdeathsbyjurisdictionandraceandHispanicorigin.csv')
		
		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State Abbreviation')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		
		#Take required columns to insert in to a target table
		df_drop_cols = df_final_join[['state_key','Week Ending Date','Race/Ethnicity','Time Period','Suppress','Note','Outcome','Number of Deaths','Average Number of Deaths in Time Period','Difference from 2015-2019 to 2020','Percent Difference from 2015-2019 to 2020','Type','created_datetime']]
		df_drop_cols[['Number of Deaths','Average Number of Deaths in Time Period','Difference from 2015-2019 to 2020','Percent Difference from 2015-2019 to 2020' ]]=df_drop_cols[['Number of Deaths','Average Number of Deaths in Time Period','Difference from 2015-2019 to 2020','Percent Difference from 2015-2019 to 2020']].fillna(value=0)
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO deaths_by_jurisdiction_race_hispanic_weekly(state_key, week_ending_date, race_ethnicity, time_period, suppress, note, outcome, number_of_deaths, average_number_of_deaths_in_time_period, difference_from_2015_2019_to_2020, percent_difference_from_2015_2019_to_2020, "Type", created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()



#Load data from sourse file into a table:US_Stateand_Territorial_Stay_AtHome_Order1
def load_US_Stateand_Territorial_Stay_AtHome_Order1():
	try:
		#Opening the postgres database connection
		pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
		connection = pg_hook.get_conn()
		cursor = connection.cursor()
		print('Database connection is Established Sucsussfully..............................................')
		cursor.execute("Truncate table US_Stateand_Territorial_Stay_AtHome_Order1")
		def get_pg_table(sql_query, database = connection):
			table = pd.read_sql_query(sql_query, database)
			return table
			
		df_states = get_pg_table("select * from states_r")
		df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/USStateTerritorialandCountyStayAtHomeOrdersMarch15May5CountyandJuly7StatebyCountybyDay.csv')
		
		#Joining the source data with the reference tables to populate keys
		df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State_Tribe_Territory')
		df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")
		
		
		#Take required columns to insert in to a target table
		df_drop_cols = df_final_join[['state_key','County_Name','FIPS_State','FIPS_County','date','Current_order_status','Order_code','Issuing_Jurisdiction','Stay_at_Home_Order_Recommendation','Effective_date','Expiration_date','Effective_NA_Reason','Expiration_NA_Reason','Date_Signed','Express_Preemption','origin_dataset','County','Source_of_Action','URL','Citation','flag','created_datetime']]
		
		#Total number of rows tobe loaded into a table
		print('Total number of rows to be loaded into table..............',df_drop_cols.shape[0])
		
		#Load the data into table
		for i, row in df_drop_cols.iterrows():
			#print(row)
			sql = """INSERT INTO US_Stateand_Territorial_Stay_AtHome_Order1(state_key, county_name, fips_state, fips_county, "date", current_order_status, order_code, issuing_jurisdiction, stay_at_home_order_recommendation, effective_date, expiration_date, effective_na_reason, expiration_na_reason, date_signed, express_preemption, origin_dataset, county, source_of_action, url, citation, flag, created_datetime)) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
			cursor.execute(sql, tuple(row))

	#Raise Exceptions			
	except connection.Error as e:
		print(e)
	except connection.DatabaseError as e:
		print(e)
	except connection.DataError as e:
		print(e)
	except connection.NotSupportedError as e:
		print(e)
	except ValueError as e:
		print(e)
	except FileNotFoundError as e:
		print(e)
	finally:
		connection.commit()
		connection.close()


def delete_Provisional_Covid_Deaths_by_County():
        os.remove('/usr/local/airflow/covid_data/AHCountylevelProvisionalCOVID19DeathsCounts.csv')

def delete_Provisional_Diabetes_Deaths():
        os.remove('/usr/local/airflow/covid_data/AHProvisionalDiabetesDeathCounts2020.csv')

def delete_Sickle_Cell_Provisional_Deaths():
        os.remove('/usr/local/airflow/covid_data/AHSickleCellDiseaseProvisionalDeathCounts20192020.csv')
		
def delete_reimbursement_to_health_care_providers():
        os.remove('/usr/local/airflow/covid_data/ClaimsReimbursementtoHealthCareProvidersandFacilitiesforTestingandTreatmentoftheUninsured.csv')
		
def delete_covid_deaths_by_condition_by_agegroup_by_state():
        os.remove('/usr/local/airflow/covid_data/Conditionscontributingtodeathsinvolvingcoronavirusdisease2019COVID19byagegroupandstateUnitedStates.csv')

def delete_covid_case_surveillance_data():
        os.remove('/usr/local/airflow/covid_data/COVID19CaseSurveillancePublicUseData.csv')

def delete_covid_impact_on_hospital_capacity():
        os.remove('/usr/local/airflow/covid_data/COVID19EstimatedPatientImpactandHospitalCapacitybyState.csv')

def delete_covid_policy_orders():
        os.remove('/usr/local/airflow/covid_data/COVID19StateandCountyPolicyOrders.csv')

def delete_covid_diagnostic_lab_testing():
        os.remove('/usr/local/airflow/covid_data/COVID19DiagnosticLaboratoryTestingPCRTestingTimeSeries.csv')

def delete_covid_impact_on_hospital_capacity_reported():
        os.remove('/usr/local/airflow/covid_data/COVID19ReportedPatientImpactandHospitalCapacitybyState.csv')
		
def delete_cumulative_provisional_countsof_deathsby_sex_race_age():
        os.remove('/usr/local/airflow/covid_data/CumulativeProvisionalCountsofDeathsbySexRaceandAge.csv')

def delete_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020():
        os.remove('/usr/local/airflow/covid_data/CumulativeProvisionalDeathCountsbySexRaceandAgethrough742020.csv')

def delete_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state():
        os.remove('/usr/local/airflow/covid_data/Deathsinvolvingcoronavirusdisease2019COVID19byraceandHispanicorigingroupandagebystate.csv')

def delete_distribution_of_covid_deaths_by_juristriction_by_age():
        os.remove('/usr/local/airflow/covid_data/DistributionofCOVID19deathsandpopulationsbyjurisdictionageandraceandHispanicorigin.csv')

def delete_excess_deaths_associated_with_covid():
        os.remove('/usr/local/airflow/covid_data/ExcessDeathsAssociatedwithCOVID19.csv')

def delete_indicators_based_on_reported_freq_symptoms():
        os.remove('/usr/local/airflow/covid_data/IndicatorsofAnxietyorDepressionBasedonReportedFrequencyofSymptomsDuringLast7Days.csv')

def delete_indicators_of_health_insurance_coverage():
        os.remove('/usr/local/airflow/covid_data/IndicatorsofHealthInsuranceCoverageattheTimeofInterview.csv')

def delete_indicators_reduced_access_care_dueto_covid():
        os.remove('/usr/local/airflow/covid_data/IndicatorsofReducedAccesstoCareDuetotheCoronavirusPandemicDuringLast4Weeks.csv')

def delete_loss_of_work_due_to_illness():
        os.remove('/usr/local/airflow/covid_data/LossofWorkDuetoIllnessRANDSduringCOVID19.csv')

def delete_mental_healthcare():
        os.remove('/usr/local/airflow/covid_data/MentalHealthCareintheLast4Weeks.csv')

def delete_monthly_covid_deaths_by_reagion_age_race():
        os.remove('/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageandraceandHispanicorigin.csv')

def delete_monthly_covid_deaths_by_region_age_race_place():
        os.remove('/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageplaceandraceandHispanicorigin.csv')

def delete_covid_deaths_by_county_and_race():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyCountyandRace.csv')

def delete_covid_deaths_by_deathplace_by_state():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyPlaceofDeathandState.csv')

def delete_covid_deaths_by_sex_age_by_state():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandState.csv')

def delete_covid_deaths_by_sex_age_by_week():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandWeek.csv')

def delete_covid_deaths_by_weekend_by_state():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyWeekEndingDateandState.csv')

def delete_covid_deaths_by_county():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsintheUnitedStatesbyCounty.csv')

def delete_covid_deaths_by_age_in_years():
        os.remove('/usr/local/airflow/covid_data/ProvisionalCOVID19DeathsCountsbyAgeinYears.csv')

def delete_Provisional_Deaths_coronavirus():
        os.remove('/usr/local/airflow/covid_data/ProvisionalDeathCountsforCoronavirusDiseaseCOVID19:DistributionofDeathsbyRaceandHispanicOrigin.csv')

def delete_provisional_death_counts_influenza_pneumonia_and_covid():
        os.remove('/usr/local/airflow/covid_data/ProvisionalDeathCountsforInfluenzaPneumoniaandCOVID19.csv')

def delete_Reduced_Access_to_Care_RANDS_during_COVID():
        os.remove('/usr/local/airflow/covid_data/ReducedAccesstoCareRANDSduringCOVID19.csv')

def delete_Telemedicine_RANDS_during_COVID19():
        os.remove('/usr/local/airflow/covid_data/TelemedicineRANDSduringCOVID19.csv')

def delete_unitedstates_covid19_cases_deaths_by_state_overtime():
        os.remove('/usr/local/airflow/covid_data/UnitedStatesCOVID19CasesandDeathsbyStateoverTime.csv')

def delete_US_Stateand_Territorial_Stay_AtHome_Order():
        os.remove('/usr/local/airflow/covid_data/USStateandTerritorialStayAtHomeOrdersMarch15July7byCountybyDay.csv')

def delete_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic():
        os.remove('/usr/local/airflow/covid_data/WeeklycountsofdeathsbyjurisdictionandraceandHispanicorigin.csv')

def delete_US_Stateand_Territorial_Stay_AtHome_Order1():
        os.remove('/usr/local/airflow/covid_data/USStateTerritorialandCountyStayAtHomeOrdersMarch15May5CountyandJuly7StatebyCountybyDay.csv')

#DAG argument settings to run at a specified time in a day.
default_args = { "owner": "airflow", "start_date": datetime.today() - timedelta(days=1) }
dag = DAG( 'covid_data_dag1', default_args=default_args,schedule_interval = "0 6 * * *" )


fetchurls = PythonOperator( task_id='fetch_url', python_callable=fetchurls, dag = dag, provide_context=True)
fetch_data_to_local = PythonOperator( task_id='fetch_data', python_callable=fetch_data_to_local, dag = dag , provide_context=True)

load_Provisional_Covid_Deaths_by_County = PythonOperator( task_id='load_Provisional_Covid_Deaths_by_County', python_callable=load_Provisional_Covid_Deaths_by_County, dag = dag )
load_Provisional_Diabetes_Deaths = PythonOperator( task_id='load_Provisional_Diabetes_Deaths', python_callable=load_Provisional_Diabetes_Deaths, dag = dag )
load_Sickle_Cell_Provisional_Deaths = PythonOperator( task_id='load_Sickle_Cell_Provisional_Deaths', python_callable=load_Sickle_Cell_Provisional_Deaths, dag = dag )
load_reimbursement_to_health_care_providers = PythonOperator( task_id='load_reimbursement_to_health_care_providers', python_callable=load_reimbursement_to_health_care_providers, dag = dag )
load_covid_deaths_by_condition_by_agegroup_by_state = PythonOperator( task_id='load_covid_deaths_by_condition_by_agegroup_by_state', python_callable=load_covid_deaths_by_condition_by_agegroup_by_state, dag = dag )
load_covid_case_surveillance_data = PythonOperator( task_id='load_covid_case_surveillance_data', python_callable=load_covid_case_surveillance_data, dag = dag )
load_covid_impact_on_hospital_capacity = PythonOperator( task_id='load_covid_impact_on_hospital_capacity', python_callable=load_covid_impact_on_hospital_capacity, dag = dag )
load_covid_policy_orders = PythonOperator( task_id='load_covid_policy_orders', python_callable=load_covid_policy_orders, dag = dag )
load_covid_diagnostic_lab_testing = PythonOperator( task_id='load_covid_diagnostic_lab_testing', python_callable=load_covid_diagnostic_lab_testing, dag = dag )
load_covid_impact_on_hospital_capacity_reported = PythonOperator( task_id='load_covid_impact_on_hospital_capacity_reported', python_callable=load_covid_impact_on_hospital_capacity_reported, dag = dag )
load_cumulative_provisional_countsof_deathsby_sex_race_age = PythonOperator( task_id='load_cumulative_provisional_countsof_deathsby_sex_race_age', python_callable=load_cumulative_provisional_countsof_deathsby_sex_race_age, dag = dag )
load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020 = PythonOperator( task_id='load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020', python_callable=load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020, dag = dag )
load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state = PythonOperator( task_id='load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state', python_callable=load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state, dag = dag )
load_distribution_of_covid_deaths_by_juristriction_by_age = PythonOperator( task_id='load_distribution_of_covid_deaths_by_juristriction_by_age', python_callable=load_distribution_of_covid_deaths_by_juristriction_by_age, dag = dag )
load_excess_deaths_associated_with_covid = PythonOperator( task_id='load_excess_deaths_associated_with_covid', python_callable=load_excess_deaths_associated_with_covid, dag = dag )
load_indicators_based_on_reported_freq_symptoms = PythonOperator( task_id='load_indicators_based_on_reported_freq_symptoms', python_callable=load_indicators_based_on_reported_freq_symptoms, dag = dag )
load_indicators_of_health_insurance_coverage = PythonOperator( task_id='load_indicators_of_health_insurance_coverage', python_callable=load_indicators_of_health_insurance_coverage, dag = dag )
load_indicators_reduced_access_care_dueto_covid = PythonOperator( task_id='load_indicators_reduced_access_care_dueto_covid', python_callable=load_indicators_reduced_access_care_dueto_covid, dag = dag )
load_loss_of_work_due_to_illness = PythonOperator( task_id='load_loss_of_work_due_to_illness', python_callable=load_loss_of_work_due_to_illness, dag = dag )
load_mental_healthcare = PythonOperator( task_id='load_mental_healthcare', python_callable=load_mental_healthcare, dag = dag )
load_monthly_covid_deaths_by_reagion_age_race = PythonOperator( task_id='load_monthly_covid_deaths_by_reagion_age_race', python_callable=load_monthly_covid_deaths_by_reagion_age_race, dag = dag )
load_monthly_covid_deaths_by_region_age_race_place = PythonOperator( task_id='load_monthly_covid_deaths_by_region_age_race_place', python_callable=load_monthly_covid_deaths_by_region_age_race_place, dag = dag )
load_covid_deaths_by_county_and_race = PythonOperator( task_id='load_covid_deaths_by_county_and_race', python_callable=load_covid_deaths_by_county_and_race, dag = dag )
load_covid_deaths_by_deathplace_by_state = PythonOperator( task_id='load_covid_deaths_by_deathplace_by_state', python_callable=load_covid_deaths_by_deathplace_by_state, dag = dag )
load_covid_deaths_by_sex_age_by_state = PythonOperator( task_id='load_covid_deaths_by_sex_age_by_state', python_callable=load_covid_deaths_by_sex_age_by_state, dag = dag )
load_covid_deaths_by_sex_age_by_week = PythonOperator( task_id='load_covid_deaths_by_sex_age_by_week', python_callable=load_covid_deaths_by_sex_age_by_week, dag = dag )
load_covid_deaths_by_weekend_by_state = PythonOperator( task_id='load_covid_deaths_by_weekend_by_state', python_callable=load_covid_deaths_by_weekend_by_state, dag = dag )
load_covid_deaths_by_county = PythonOperator( task_id='load_covid_deaths_by_county', python_callable=load_covid_deaths_by_county, dag = dag )
load_covid_deaths_by_age_in_years = PythonOperator( task_id='load_covid_deaths_by_age_in_years', python_callable=load_covid_deaths_by_age_in_years, dag = dag )
load_Provisional_Deaths_coronavirus = PythonOperator( task_id='load_Provisional_Deaths_coronavirus', python_callable=load_Provisional_Deaths_coronavirus, dag = dag )
load_provisional_death_counts_influenza_pneumonia_and_covid = PythonOperator( task_id='load_provisional_death_counts_influenza_pneumonia_and_covid', python_callable=load_provisional_death_counts_influenza_pneumonia_and_covid, dag = dag )
load_Reduced_Access_to_Care_RANDS_during_COVID = PythonOperator( task_id='load_Reduced_Access_to_Care_RANDS_during_COVID', python_callable=load_Reduced_Access_to_Care_RANDS_during_COVID, dag = dag )
load_Telemedicine_RANDS_during_COVID19 = PythonOperator( task_id='load_Telemedicine_RANDS_during_COVID19', python_callable=load_Telemedicine_RANDS_during_COVID19, dag = dag )
load_unitedstates_covid19_cases_deaths_by_state_overtime = PythonOperator( task_id='load_unitedstates_covid19_cases_deaths_by_state_overtime', python_callable=load_unitedstates_covid19_cases_deaths_by_state_overtime, dag = dag )
load_US_Stateand_Territorial_Stay_AtHome_Order = PythonOperator( task_id='load_US_Stateand_Territorial_Stay_AtHome_Order', python_callable=load_US_Stateand_Territorial_Stay_AtHome_Order, dag = dag )
load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic = PythonOperator( task_id='load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic', python_callable=load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic, dag = dag )
load_US_Stateand_Territorial_Stay_AtHome_Order1 = PythonOperator( task_id='load_US_Stateand_Territorial_Stay_AtHome_Order1', python_callable=load_US_Stateand_Territorial_Stay_AtHome_Order1, dag = dag )
delete_Provisional_Covid_Deaths_by_County = PythonOperator( task_id='delete_Provisional_Covid_Deaths_by_County', python_callable=delete_Provisional_Covid_Deaths_by_County, dag = dag )
delete_Provisional_Diabetes_Deaths = PythonOperator( task_id='delete_Provisional_Diabetes_Deaths', python_callable=delete_Provisional_Diabetes_Deaths, dag = dag )
delete_Sickle_Cell_Provisional_Deaths = PythonOperator( task_id='delete_Sickle_Cell_Provisional_Deaths', python_callable=delete_Sickle_Cell_Provisional_Deaths, dag = dag )
delete_reimbursement_to_health_care_providers = PythonOperator( task_id='delete_reimbursement_to_health_care_providers', python_callable=delete_reimbursement_to_health_care_providers, dag = dag )
delete_covid_deaths_by_condition_by_agegroup_by_state = PythonOperator( task_id='delete_covid_deaths_by_condition_by_agegroup_by_state', python_callable=delete_covid_deaths_by_condition_by_agegroup_by_state, dag = dag )
delete_covid_case_surveillance_data = PythonOperator( task_id='delete_covid_case_surveillance_data', python_callable=delete_covid_case_surveillance_data, dag = dag )
delete_covid_impact_on_hospital_capacity = PythonOperator( task_id='delete_covid_impact_on_hospital_capacity', python_callable=delete_covid_impact_on_hospital_capacity, dag = dag )
delete_covid_policy_orders = PythonOperator( task_id='delete_covid_policy_orders', python_callable=delete_covid_policy_orders, dag = dag )
delete_covid_diagnostic_lab_testing = PythonOperator( task_id='delete_covid_diagnostic_lab_testing', python_callable=delete_covid_diagnostic_lab_testing, dag = dag )
delete_covid_impact_on_hospital_capacity_reported = PythonOperator( task_id='delete_covid_impact_on_hospital_capacity_reported', python_callable=delete_covid_impact_on_hospital_capacity_reported, dag = dag )
delete_cumulative_provisional_countsof_deathsby_sex_race_age = PythonOperator( task_id='delete_cumulative_provisional_countsof_deathsby_sex_race_age', python_callable=delete_cumulative_provisional_countsof_deathsby_sex_race_age, dag = dag )
delete_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020 = PythonOperator( task_id='delete_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020', python_callable=delete_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020, dag = dag )
delete_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state = PythonOperator( task_id='delete_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state', python_callable=delete_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state, dag = dag )
delete_distribution_of_covid_deaths_by_juristriction_by_age = PythonOperator( task_id='delete_distribution_of_covid_deaths_by_juristriction_by_age', python_callable=delete_distribution_of_covid_deaths_by_juristriction_by_age, dag = dag )
delete_excess_deaths_associated_with_covid = PythonOperator( task_id='delete_excess_deaths_associated_with_covid', python_callable=delete_excess_deaths_associated_with_covid, dag = dag )
delete_indicators_based_on_reported_freq_symptoms = PythonOperator( task_id='delete_indicators_based_on_reported_freq_symptoms', python_callable=delete_indicators_based_on_reported_freq_symptoms, dag = dag )
delete_indicators_of_health_insurance_coverage = PythonOperator( task_id='delete_indicators_of_health_insurance_coverage', python_callable=delete_indicators_of_health_insurance_coverage, dag = dag )
delete_indicators_reduced_access_care_dueto_covid = PythonOperator( task_id='delete_indicators_reduced_access_care_dueto_covid', python_callable=delete_indicators_reduced_access_care_dueto_covid, dag = dag )
delete_loss_of_work_due_to_illness = PythonOperator( task_id='delete_loss_of_work_due_to_illness', python_callable=delete_loss_of_work_due_to_illness, dag = dag )
delete_mental_healthcare = PythonOperator( task_id='delete_mental_healthcare', python_callable=delete_mental_healthcare, dag = dag )
delete_monthly_covid_deaths_by_reagion_age_race = PythonOperator( task_id='delete_monthly_covid_deaths_by_reagion_age_race', python_callable=delete_monthly_covid_deaths_by_reagion_age_race, dag = dag )
delete_monthly_covid_deaths_by_region_age_race_place = PythonOperator( task_id='delete_monthly_covid_deaths_by_region_age_race_place', python_callable=delete_monthly_covid_deaths_by_region_age_race_place, dag = dag )
delete_covid_deaths_by_county_and_race = PythonOperator( task_id='delete_covid_deaths_by_county_and_race', python_callable=delete_covid_deaths_by_county_and_race, dag = dag )
delete_covid_deaths_by_deathplace_by_state = PythonOperator( task_id='delete_covid_deaths_by_deathplace_by_state', python_callable=delete_covid_deaths_by_deathplace_by_state, dag = dag )
delete_covid_deaths_by_sex_age_by_state = PythonOperator( task_id='delete_covid_deaths_by_sex_age_by_state', python_callable=delete_covid_deaths_by_sex_age_by_state, dag = dag )
delete_covid_deaths_by_sex_age_by_week = PythonOperator( task_id='delete_covid_deaths_by_sex_age_by_week', python_callable=delete_covid_deaths_by_sex_age_by_week, dag = dag )
delete_covid_deaths_by_weekend_by_state = PythonOperator( task_id='delete_covid_deaths_by_weekend_by_state', python_callable=delete_covid_deaths_by_weekend_by_state, dag = dag )
delete_covid_deaths_by_county = PythonOperator( task_id='delete_covid_deaths_by_county', python_callable=delete_covid_deaths_by_county, dag = dag )
delete_covid_deaths_by_age_in_years = PythonOperator( task_id='delete_covid_deaths_by_age_in_years', python_callable=delete_covid_deaths_by_age_in_years, dag = dag )
delete_Provisional_Deaths_coronavirus = PythonOperator( task_id='delete_Provisional_Deaths_coronavirus', python_callable=delete_Provisional_Deaths_coronavirus, dag = dag )
delete_provisional_death_counts_influenza_pneumonia_and_covid = PythonOperator( task_id='delete_provisional_death_counts_influenza_pneumonia_and_covid', python_callable=delete_provisional_death_counts_influenza_pneumonia_and_covid, dag = dag )
delete_Reduced_Access_to_Care_RANDS_during_COVID = PythonOperator( task_id='delete_Reduced_Access_to_Care_RANDS_during_COVID', python_callable=delete_Reduced_Access_to_Care_RANDS_during_COVID, dag = dag )
delete_Telemedicine_RANDS_during_COVID19 = PythonOperator( task_id='delete_Telemedicine_RANDS_during_COVID19', python_callable=delete_Telemedicine_RANDS_during_COVID19, dag = dag )
delete_unitedstates_covid19_cases_deaths_by_state_overtime = PythonOperator( task_id='delete_unitedstates_covid19_cases_deaths_by_state_overtime', python_callable=delete_unitedstates_covid19_cases_deaths_by_state_overtime, dag = dag )
delete_US_Stateand_Territorial_Stay_AtHome_Order = PythonOperator( task_id='delete_US_Stateand_Territorial_Stay_AtHome_Order', python_callable=delete_US_Stateand_Territorial_Stay_AtHome_Order, dag = dag )
delete_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic = PythonOperator( task_id='delete_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic', python_callable=delete_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic, dag = dag )
delete_US_Stateand_Territorial_Stay_AtHome_Order1 = PythonOperator( task_id='delete_US_Stateand_Territorial_Stay_AtHome_Order1', python_callable=delete_US_Stateand_Territorial_Stay_AtHome_Order1, dag = dag )


#Setting dependencies between the tasks
fetchurls >> fetch_data_to_local
fetch_data_to_local >> [  load_Provisional_Covid_Deaths_by_County,load_Provisional_Diabetes_Deaths,load_Sickle_Cell_Provisional_Deaths,load_reimbursement_to_health_care_providers,load_covid_deaths_by_condition_by_agegroup_by_state,load_covid_case_surveillance_data,load_covid_impact_on_hospital_capacity,load_covid_policy_orders,load_covid_diagnostic_lab_testing,load_covid_impact_on_hospital_capacity_reported,load_cumulative_provisional_countsof_deathsby_sex_race_age,load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020,load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state,load_distribution_of_covid_deaths_by_juristriction_by_age,load_excess_deaths_associated_with_covid,load_indicators_based_on_reported_freq_symptoms,load_indicators_of_health_insurance_coverage,load_indicators_reduced_access_care_dueto_covid,load_loss_of_work_due_to_illness,load_mental_healthcare,load_monthly_covid_deaths_by_reagion_age_race,load_monthly_covid_deaths_by_region_age_race_place,load_covid_deaths_by_county_and_race,load_covid_deaths_by_deathplace_by_state,load_covid_deaths_by_sex_age_by_state,load_covid_deaths_by_sex_age_by_week,load_covid_deaths_by_weekend_by_state,load_covid_deaths_by_county,load_covid_deaths_by_age_in_years,load_Provisional_Deaths_coronavirus,load_provisional_death_counts_influenza_pneumonia_and_covid,load_Reduced_Access_to_Care_RANDS_during_COVID,load_Telemedicine_RANDS_during_COVID19,load_unitedstates_covid19_cases_deaths_by_state_overtime,load_US_Stateand_Territorial_Stay_AtHome_Order,load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic,load_US_Stateand_Territorial_Stay_AtHome_Order1 ] 
load_Provisional_Covid_Deaths_by_County >> delete_Provisional_Covid_Deaths_by_County
load_Provisional_Diabetes_Deaths >> delete_Provisional_Diabetes_Deaths
load_Sickle_Cell_Provisional_Deaths >> delete_Sickle_Cell_Provisional_Deaths
load_reimbursement_to_health_care_providers >> delete_reimbursement_to_health_care_providers
load_covid_deaths_by_condition_by_agegroup_by_state >> delete_covid_deaths_by_condition_by_agegroup_by_state
load_covid_case_surveillance_data >> delete_covid_case_surveillance_data
load_covid_impact_on_hospital_capacity >> delete_covid_impact_on_hospital_capacity
load_covid_policy_orders >> delete_covid_policy_orders
load_covid_diagnostic_lab_testing >> delete_covid_diagnostic_lab_testing
load_covid_impact_on_hospital_capacity_reported >> delete_covid_impact_on_hospital_capacity_reported
load_cumulative_provisional_countsof_deathsby_sex_race_age >> delete_cumulative_provisional_countsof_deathsby_sex_race_age
load_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020 >> delete_cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020
load_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state >> delete_deaths_involving_covid_by_race_hispanic_originggroup_age_by_state
load_distribution_of_covid_deaths_by_juristriction_by_age >> delete_distribution_of_covid_deaths_by_juristriction_by_age
load_excess_deaths_associated_with_covid >> delete_excess_deaths_associated_with_covid
load_indicators_based_on_reported_freq_symptoms >> delete_indicators_based_on_reported_freq_symptoms
load_indicators_of_health_insurance_coverage >> delete_indicators_of_health_insurance_coverage
load_indicators_reduced_access_care_dueto_covid >> delete_indicators_reduced_access_care_dueto_covid
load_loss_of_work_due_to_illness >> delete_loss_of_work_due_to_illness
load_mental_healthcare >> delete_mental_healthcare
load_monthly_covid_deaths_by_reagion_age_race >> delete_monthly_covid_deaths_by_reagion_age_race
load_monthly_covid_deaths_by_region_age_race_place >> delete_monthly_covid_deaths_by_region_age_race_place
load_covid_deaths_by_county_and_race >> delete_covid_deaths_by_county_and_race
load_covid_deaths_by_deathplace_by_state >> delete_covid_deaths_by_deathplace_by_state
load_covid_deaths_by_sex_age_by_state >> delete_covid_deaths_by_sex_age_by_state
load_covid_deaths_by_sex_age_by_week >> delete_covid_deaths_by_sex_age_by_week
load_covid_deaths_by_weekend_by_state >> delete_covid_deaths_by_weekend_by_state
load_covid_deaths_by_county >> delete_covid_deaths_by_county
load_covid_deaths_by_age_in_years >> delete_covid_deaths_by_age_in_years
load_Provisional_Deaths_coronavirus >> delete_Provisional_Deaths_coronavirus
load_provisional_death_counts_influenza_pneumonia_and_covid >> delete_provisional_death_counts_influenza_pneumonia_and_covid
load_Reduced_Access_to_Care_RANDS_during_COVID >> delete_Reduced_Access_to_Care_RANDS_during_COVID
load_Telemedicine_RANDS_during_COVID19 >> delete_Telemedicine_RANDS_during_COVID19
load_unitedstates_covid19_cases_deaths_by_state_overtime >> delete_unitedstates_covid19_cases_deaths_by_state_overtime
load_US_Stateand_Territorial_Stay_AtHome_Order >> delete_US_Stateand_Territorial_Stay_AtHome_Order
load_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic >> delete_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic
load_US_Stateand_Territorial_Stay_AtHome_Order1 >> delete_US_Stateand_Territorial_Stay_AtHome_Order1

