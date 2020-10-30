"""
Covid data engineering project 
"""
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
def load_covid_impact_on_hospital_capacity():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_impact_on_hospital_capacity'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19EstimatedPatientImpactandHospitalCapacitybyState.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()
        df_incr_list=[]

        #Change the calendar column name as per the table load
        df_loadcsv_list= df_loadcsv['collection_date'].tolist()
        for val in df_loadcsv_list:
                ax=datetime.strptime(val,'%Y-%m-%d').date()
                df_incr_list.append(ax)

        #Change the calendar column name as per the table load
        df_loadcsv['collection_date'] = df_incr_list
        df_loadcsv = df_loadcsv[df_loadcsv['collection_date']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
        df_join_states['date_format'] = pd.to_datetime(df_join_states.collection_date)
        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='date_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['state_key','calendar_key','Inpatient Beds Occupied Estimated','Count LL','Count UL','Percentage of Inpatient Beds Occupied Estimated','Percentage LL','Percentage UL','Total Inpatient Beds','Total LL','Total UL','created_datetime']]
        df_drop_cols = df_drop_cols.apply(lambda x: x.replace(',',''))
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                sql = "INSERT INTO public.covid_impact_on_hospital_capacity(state_key, calendar_key, inpatient_beds_occupied_estimated, count_ll, count_ul, percentage_of_inpatient_beds_occupied_estimated, percentage_ll, percentage_ul, total_inpatient_beds, total_ll, total_ul, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s)"
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_diagnostic_lab_testing'  "
        if df_final_join['collection_date'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['collection_date'].max() , pd.to_datetime('today').strftime("%Y-%m-%d") ) )

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


#src filename:AH County-level Provisional COVID-19 Deaths Counts.csv
#target table name: Provisional_Covid_Deaths_by_County
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

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['EndWeek']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states.EndWeek)
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states.StartWeek)

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','County','Fips Code','COVID-19 Deaths','Total Deaths','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO Provisional_Covid_Deaths_by_County(week_start_calendar_key,
                week_end_calendar_key,state_key,County,Fips_Code,COVID_19_Deaths,Total_Deaths,Created_Datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_covid_deaths_by_county' "
        if df_final_join['EndWeek'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['EndWeek'].max() , df_final_join['EndWeek'].max()) )

        connection.commit()
        connection.close()


#Function to load the table Provisional_Diabetes_Deaths
def load_Provisional_Diabetes_Deaths():
	pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()

	def get_pg_table(sql_query, database = connection):
		table = pd.read_sql_query(sql_query, database)
		return table

	df_calendar = get_pg_table("select * from calendar_r")

	#Change the select query where clause as per the table load
	df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'provisional_diabetes_deaths'")

	#Change the path of the source file as per the table load
	df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHProvisionalDiabetesDeathCounts2020.csv')
	pickup_value = df_pickupval['pickup_criteria'].tolist()
	df_incr_list=[]

	#Change the calendar column name as per the table load
	df_loadcsv_list= df_loadcsv['Data as of'].tolist()
	for val in df_loadcsv_list:
		ax=datetime.strptime(val,'%m/%d/%Y').date()
		df_incr_list.append(ax)

	#Change the calendar column name as per the table load
	df_loadcsv['Data as of'] = df_incr_list
	df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

	df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
	df_loadcsv['date_format'] = pd.to_datetime(df_loadcsv['Data as of'])
	df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='date_format', right_on='date_format')
	df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

	#Change with the required column names
	df_drop_cols = df_final_join[['calendar_key','Date_Of_Death_Year','Date_Of_Death_Month','AgeGroup','Sex','COVID19','Diabetes.uc','Diabetes.mc','C19PlusDiabetes','C19PlusHypertensiveDiseases','C19PlusMajorCardiovascularDiseases','C19PlusHypertensiveDiseasesAndMCVD','C19PlusChronicLowerRespiratoryDisease','C19PlusKidneyDisease','C19PlusChronicLiverDiseaseAndCirrhosis','C19PlusObesity', 'created_datetime']]

	#Change with the required column names
	for i, row in df_drop_cols.iterrows():
		sql = """INSERT INTO public.provisional_diabetes_deaths
(calendar_key, date_of_death_year, date_of_death_month, agegroup, sex, covid19, diabetes_uc, diabetes_mc, c19plusdiabetes, c19plushypertensivediseases, c19plusmajorcardiovasculardiseases, c19plushypertensivediseasesandmcvd, c19pluschroniclowerrespiratorydisease, c19pluskidneydisease, c19pluschronicliverdiseaseandcirrhosis, c19plusobesity, created_datetime)
 VALUES(%s,%s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
		cursor.execute(sql, tuple(row))

	#Change the update query where clause as per the table load
        
	sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='provisional_diabetes_deaths' "
	if df_final_join['Data as of'].count() > 0 :
		cursor.execute(sql_upd, (df_final_join['Data as of'].max() , df_final_join['Data as of'].max()) )

	connection.commit()
	connection.close()

#Function to load Sickle_Cell_Provisional_Deaths
def load_Sickle_Cell_Provisional_Deaths():
	pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()

	def get_pg_table(sql_query, database = connection):
		table = pd.read_sql_query(sql_query, database)
		return table

	df_calendar = get_pg_table("select * from calendar_r")

	#Change the select query where clause as per the table load
	df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'sickle_cell_provisional_deaths'")

	#Change the path of the source file as per the table load
	df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/AHSickleCellDiseaseProvisionalDeathCounts20192020.csv')
	pickup_value = df_pickupval['pickup_criteria'].tolist()
	df_incr_list=[]

	#Change the calendar column name as per the table load
	df_loadcsv_list= df_loadcsv['Data as of'].tolist()
	for val in df_loadcsv_list:
		ax=datetime.strptime(val,'%m/%d/%Y').date()
		df_incr_list.append(ax)

	#Change the calendar column name as per the table load
	df_loadcsv['Data as of'] = df_incr_list
	df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

	df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
	df_loadcsv['date_format'] = pd.to_datetime(df_loadcsv['Data as of'])
	df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='date_format', right_on='date_format')
	df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

	#Change with the required column names
	df_drop_cols = df_final_join[['calendar_key','Date of Death Year','Quarter','Race or Hispanic Origin','Age Group','SCD_Underlying','SCD_Multi','SCD and COVID-19','created_datetime']]

	#Change with the required column names
	for i, row in df_drop_cols.iterrows():
		sql = """INSERT INTO public.sickle_cell_provisional_deaths
(calendar_key, date_of_death_year, quarter, race_or_hispanic_origin, age_group, scd_underlying, scd_multi, scd_and_covid_19, created_datetime)
 VALUES(%s,%s, %s, %s,%s, %s, %s, %s, %s)"""
		cursor.execute(sql, tuple(row))

	#Change the update query where clause as per the table load
        
	sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='sickle_cell_provisional_deaths' "
	if df_final_join['Data as of'].count() > 0 :
		cursor.execute(sql_upd, (df_final_join['Data as of'].max() , df_final_join['Data as of'].max()) )

	connection.commit()
	connection.close()


#Function to load the table
def load_reimbursement_to_health_care_providers():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()		
        cursor.execute(""" truncate table reimbursement_to_health_care_providers """)
		
        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")

        #Change the select query where clause as per the table load
        #df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'reimbursement_to_health_care_providers'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ClaimsReimbursementtoHealthCareProvidersandFacilitiesforTestingandTreatmentoftheUninsured.csv')

        #Change the column names as per the table load
        df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['Provider Name','state_key','City','Claims Paid for Testing','Claims Paid for Treatment','Georeferenced Column','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                sql = """INSERT INTO public.reimbursement_to_health_care_providers
(provider_name, state_key, city, claims_paid_for_testing, claims_paid_for_treatment, georeferenced_column, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s)"""
                cursor.execute(sql, tuple(row))

        connection.commit()
        connection.close()

#Function to load the table
def load_covid_impact_on_hospital_capacity_reported():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()		
        cursor.execute(""" truncate table covid_impact_on_hospital_capacity_reported """)
		
        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")

        #Change the select query where clause as per the table load
        #df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_impact_on_hospital_capacity_reported'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19ReportedPatientImpactandHospitalCapacitybyState.csv')

        #Change the column names as per the table load
        df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='state')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['state_key','hospital_onset_covid','hospital_onset_covid_coverage','inpatient_beds','inpatient_beds_coverage','inpatient_beds_used','inpatient_beds_used_coverage','inpatient_beds_used_covid','inpatient_beds_used_covid_coverage','staffed_adult_icu_bed_occupancy','staffed_adult_icu_bed_occupancy_coverage','staffed_icu_adult_patients_confirmed_and_suspected_covid','staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage','staffed_icu_adult_patients_confirmed_covid','staffed_icu_adult_patients_confirmed_covid_coverage','total_adult_patients_hospitalized_confirmed_and_suspected_covid','total_adult_patients_hospitalized_confirmed_covid','total_adult_patients_hospitalized_confirmed_covid_coverage','total_pediatric_patients_hospitalized_confirmed_and_suspected_covid','total_pediatric_patients_hospitalized_confirmed_covid','total_pediatric_patients_hospitalized_confirmed_covid_coverage','total_staffed_adult_icu_beds','total_staffed_adult_icu_beds_coverage','inpatient_beds_utilization','inpatient_beds_utilization_coverage','inpatient_beds_utilization_numerator','inpatient_beds_utilization_denominator','percent_of_inpatients_with_covid','percent_of_inpatients_with_covid_coverage','percent_of_inpatients_with_covid_numerator','percent_of_inpatients_with_covid_denominator','inpatient_bed_covid_utilization','inpatient_bed_covid_utilization_coverage','inpatient_bed_covid_utilization_numerator','inpatient_bed_covid_utilization_denominator','adult_icu_bed_covid_utilization','adult_icu_bed_covid_utilization_coverage','adult_icu_bed_covid_utilization_numerator','adult_icu_bed_covid_utilization_denominator','adult_icu_bed_utilization','adult_icu_bed_utilization_coverage','adult_icu_bed_utilization_numerator','adult_icu_bed_utilization_denominator','reporting_cutoff_start','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                sql = """INSERT INTO public.covid_impact_on_hospital_capacity_reported(state_key, hospital_onset_covid, hospital_onset_covid_coverage, inpatient_beds, inpatient_beds_coverage, inpatient_beds_used, inpatient_beds_used_coverage, inpatient_beds_used_covid, inpatient_beds_used_covid_coverage, staffed_adult_icu_bed_occupancy, staffed_adult_icu_bed_occupancy_coverage, staffed_icu_adult_patients_confirmed_and_suspected_covid, staffed_icu_adult_patients_confirmed_and_suspected_covid_covera, staffed_icu_adult_patients_confirmed_covid, staffed_icu_adult_patients_confirmed_covid_coverage, total_adult_patients_hospitalized_confirmed_and_suspected_covid, total_adult_patients_hospitalized_confirmed_covid, total_adult_patients_hospitalized_confirmed_covid_coverage, total_pediatric_patients_hospitalized_confirmed_and_suspected_c, total_pediatric_patients_hospitalized_confirmed_covid, total_pediatric_patients_hospitalized_confirmed_covid_coverage, total_staffed_adult_icu_beds, total_staffed_adult_icu_beds_coverage, inpatient_beds_utilization, inpatient_beds_utilization_coverage, inpatient_beds_utilization_numerator, inpatient_beds_utilization_denominator, percent_of_inpatients_with_covid, percent_of_inpatients_with_covid_coverage, percent_of_inpatients_with_covid_numerator, percent_of_inpatients_with_covid_denominator, inpatient_bed_covid_utilization, inpatient_bed_covid_utilization_coverage, inpatient_bed_covid_utilization_numerator, inpatient_bed_covid_utilization_denominator, adult_icu_bed_covid_utilization, adult_icu_bed_covid_utilization_coverage, adult_icu_bed_covid_utilization_numerator, adult_icu_bed_covid_utilization_denominator, adult_icu_bed_utilization, adult_icu_bed_utilization_coverage, adult_icu_bed_utilization_numerator, adult_icu_bed_utilization_denominator, reporting_cutoff_start, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s,%s, %s, %s)"""
                cursor.execute(sql, tuple(row))

        connection.commit()
        connection.close()

#target table name: Provisional_Covid_Deaths_by_County
def load_covid_case_surveillance_data():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_case_surveillance_data'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/COVID19CaseSurveillancePublicUseData.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_cdc_dt=[]
        df_cdc_dt=df_loadcsv['cdc_report_dt'].tolist()
        for x in df_cdc_dt:
                date_formatted=datetime.strptime(x,'%Y/%m/%d').date()
                df_list_cdc_dt.append(date_formatted)

        df_loadcsv['cdc_report_dt']=df_list_cdc_dt

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['cdc_report_dt']>pickup_value[0]]

        #Change the column names as per the table load
        df_loadcsv['cdc_report_dt_format'] = pd.to_datetime(df_loadcsv.cdc_report_dt)
        df_loadcsv['pos_spec_dt_format'] = pd.to_datetime(df_loadcsv.pos_spec_dt)

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='cdc_report_dt_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='pos_spec_dt_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','onset_dt','current_status','sex','age_group','Race and ethnicity (combined)','hosp_yn','icu_yn','death_yn','medcond_yn','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_case_surveillance_data
(cdc_report_key, pos_spec_key, onset_dt, current_status, sex, age_group, race_and_ethnicity, hosp_yn, icu_yn, death_yn, medcond_yn, created_datetime) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_case_surveillance_data' "
        if df_final_join['cdc_report_dt'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['cdc_report_dt'].max() , pd.to_datetime('today').strftime("%Y-%m-%d") ) )

        connection.commit()
        connection.close()	
	
#target table name: Provisional_Covid_Deaths_by_County
def load_covid_deaths_by_condition_by_agegroup_by_state():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_condition_by_agegroup_by_state'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/Conditionscontributingtodeathsinvolvingcoronavirusdisease2019COVID19byagegroupandstateUnitedStates.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start Week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Condition Group','Condition','ICD10_codes','Age Group','Number of COVID-19 Deaths','Flag','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_condition_by_agegroup_by_state
(week_start_calendar_key, week_end_calendar_key, state_key, date_as_of, condition_group, "condition", icd10_codes, age_group, number_of_covid_19_deaths, flag, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_condition_by_agegroup_by_state' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_county_and_race():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_county_and_race'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyCountyandRace.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','County Name','Urban Rural Code','FIPS State','FIPS County','FIPS Code','Indicator','Total deaths','COVID-19 Deaths','Non-Hispanic White','Non-Hispanic Black','Non-Hispanic American Indian or Alaska Native','Non-Hispanic Asian','Other','Hispanic','Urban Rural Description','Footnote','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_county_and_race
(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, county_name, urban_rural_code, fips_state, fips_county, fips_code, "indicator", total_deaths, covid_19_deaths, non_hispanic_white, non_hispanic_black, non_hispanic_american_indian_or_alaska_native, non_hispanic_asian, other, hispanic, urban_rural_description, footnote, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_county_and_race' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_deathplace_by_state():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_deathplace_by_state'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyPlaceofDeathandState.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Place of Death','COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths','Footnote','created_datetime']]
        df_drop_cols[[ 'COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths' ]] = df_drop_cols[[ 'COVID19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID19 Deaths' ]].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_deathplace_by_state
(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, place_of_death, covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_deathplace_by_state' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_sex_age_by_state():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_sex_age_by_state'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandState.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Sex','Age group','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
        df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]] = df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_sex_age_by_state
(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, sex, age_group,covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_sex_age_by_state' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_sex_age_by_week():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_sex_age_by_week'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbySexAgeandWeek.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key','state_key','Data as of','Sex','Age Group','COVID-19 Deaths','Total Deaths','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_sex_age_by_week
(week_end_calendar_key, state_key, data_as_of, sex, age_group, covid19_deaths, total_deaths, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_sex_age_by_week' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()


#function 
def load_covid_deaths_by_weekend_by_state():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_weekend_by_state'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsbyWeekEndingDateandState.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_name', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['End Week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['Start week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Data as of','Group','Indicator','COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths','Footnote','created_datetime']]
        df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]] = df_drop_cols[[ 'COVID-19 Deaths','Total Deaths','Pneumonia Deaths','Pneumonia and COVID-19 Deaths','Influenza Deaths','Pneumonia, Influenza, or COVID-19 Deaths' ]].fillna(0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_weekend_by_state
(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, "group", "Indicator",covid19_deaths, total_deaths, pneumonia_deaths, pneumonia_and_covid19_deaths, influenza_deaths, pneumonia_influenza_or_covid19_deaths, footnote, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_weekend_by_state' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_county():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")
        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_county'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathCountsintheUnitedStatesbyCounty.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Date as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Date as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Date as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_join_states = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_join_states['EndWeekdate_format'] = pd.to_datetime(df_join_states['Last week'])
        df_join_states['StartWeekdate_format'] = pd.to_datetime(df_join_states['First week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_join_states, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','state_key','Date as of','County name','FIPS County Code','Deaths involving COVID-19','Deaths from All Causes','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_county
(week_start_calendar_key, week_end_calendar_key, state_key, data_as_of, county_name, fips_county, deaths_involving_covid_19, deaths_from_all_causes, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_county' "
        if df_final_join['Date as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Date as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_covid_deaths_by_age_in_years():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_calendar = get_pg_table("select * from calendar_r")

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'covid_deaths_by_age_in_years'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/ProvisionalCOVID19DeathsCountsbyAgeinYears.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        #Change the column names as per the table load
        df_loadcsv['EndWeekdate_format'] = pd.to_datetime(df_loadcsv['End Week'])
        df_loadcsv['StartWeekdate_format'] = pd.to_datetime(df_loadcsv['Start Week'])

        df_calendar['date_format'] = pd.to_datetime(df_calendar.calendar_date)
        df_final_join = pd.merge(df_loadcsv, df_calendar, how='inner', left_on='EndWeekdate_format', right_on='date_format')
        df_final_join = pd.merge(df_final_join, df_calendar, how='inner', left_on='StartWeekdate_format', right_on='date_format')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['calendar_key_y','calendar_key_x','Data as of','Sex','Age Years','Total deaths','COVID-19 Deaths','created_datetime']]

        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.covid_deaths_by_age_in_years
(week_start_calendar_key, week_end_calendar_key, data_as_of, sex, age_years, total_deaths, covid19_deaths, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='covid_deaths_by_age_in_years' "
        if df_final_join['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_final_join['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()


#function 
def load_monthly_covid_deaths_by_reagion_age_race():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'monthly_covid_deaths_by_reagion_age_race'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageandraceandHispanicorigin.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['Data as of'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%m/%d/%Y').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['Data as of']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['Data as of']>pickup_value[0]]

        df_loadcsv['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_loadcsv[['Data as of','Date Of Death Year','Date Of Death Month','Region','AgeGroup','RaceEthnicity','COVID-19 (U071, Multiple Cause of Death)','Sex','Place Of Death','Note1','Note2','created_datetime']]
        df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'] = df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.monthly_covid_deaths_by_reagion_age_race
(data_as_of, date_of_death_year, date_of_death_month, region, agegroup, raceethnicity, covid_19_u071_multiple_cause_of_death, sex, place_of_death, note1, note2, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='monthly_covid_deaths_by_reagion_age_race' "
        if df_drop_cols['Data as of'].count() > 0 :
                cursor.execute(sql_upd, (df_drop_cols['Data as of'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()

#function 
def load_monthly_covid_deaths_by_region_age_race_place():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        #Change the select query where clause as per the table load
        df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'monthly_covid_deaths_by_region_age_race_place'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageplaceandraceandHispanicorigin.csv')
        pickup_value = df_pickupval['pickup_criteria'].tolist()

        df_list_DataasOf=[]
        df_DataasOf=df_loadcsv['AnalysisDate'].tolist()
        for x in df_DataasOf:
                date_formatted=datetime.strptime(x,'%Y-%m-%d').date()
                df_list_DataasOf.append(date_formatted)

        df_loadcsv['AnalysisDate']=df_list_DataasOf

        #Change the calendar column name as per the table load
        df_loadcsv = df_loadcsv[df_loadcsv['AnalysisDate']>pickup_value[0]]

        df_loadcsv['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_loadcsv[['AnalysisDate','Date Of Death Year','Date Of Death Month','Region','AgeGroup','RaceEthnicity','COVID-19 (U071, Multiple Cause of Death)','Sex','Place Of Death','Note','flag_cov19mcod','created_datetime']]
        df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'] = df_drop_cols['COVID-19 (U071, Multiple Cause of Death)'].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                #print(row)
                sql = """INSERT INTO public.monthly_covid_deaths_by_region_age_race_place
(AnalysisDate, date_of_death_year, date_of_death_month, region, agegroup, raceethnicity, covid_19_u071_multiple_cause_of_death, sex, place_of_death, note, flag_cov19mcod, created_datetime)
 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql, tuple(row))

        #Change the update query where clause as per the table load
        sql_upd = "UPDATE public.project_audit_table SET  pickup_criteria=%s, run_date=%s WHERE table_name='monthly_covid_deaths_by_region_age_race_place' "
        if df_drop_cols['AnalysisDate'].count() > 0 :
                cursor.execute(sql_upd, (df_drop_cols['AnalysisDate'].max() , pd.to_datetime('today').strftime("%Y-%m-%d")) )

        connection.commit()
        connection.close()


#Function to load the table
def load_indicators_based_on_reported_freq_symptoms():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()		
        cursor.execute(""" truncate table load_indicators_based_on_reported_freq_symptoms """)
		
        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")

        #Change the select query where clause as per the table load
        #df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'reimbursement_to_health_care_providers'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/IndicatorsofAnxietyorDepressionBasedonReportedFrequencyofSymptomsDuringLast7Days.csv')

        #Change the column names as per the table load
        df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['Phase','Indicator','Group','State','Subgroup','Time Period','Time Period Label','Value','Low CI','High CI','Confidence Interval','Quartile range','created_datetime']]
        df_drop_cols[[ 'Value','Low CI','High CI'  ]] = df_drop_cols[[ 'Value','Low CI','High CI'  ]].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                sql = """INSERT INTO public.load_indicators_based_on_reported_freq_symptoms
(phase, "Indicator", "Group", state, subgroup, time_period, time_period_label, value, low_ci, high_ci, confidence_interval, quartile_range, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s)"""
                cursor.execute(sql, tuple(row))

        connection.commit()
        connection.close()


#Function to load the table
def load_indicators_of_health_insurance_coverage():
        pg_hook = PostgresHook(postgres_conn_id='postgres_covid')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()		
        cursor.execute(""" truncate table indicators_of_health_insurance_coverage """)
		
        def get_pg_table(sql_query, database = connection):
                table = pd.read_sql_query(sql_query, database)
                return table

        df_states = get_pg_table("select * from states_r")

        #Change the select query where clause as per the table load
        #df_pickupval = get_pg_table("select pickup_criteria from project_audit_table where table_name = 'reimbursement_to_health_care_providers'")

        #Change the path of the source file as per the table load
        df_loadcsv = pd.read_csv(r'/usr/local/airflow/covid_data/IndicatorsofHealthInsuranceCoverageattheTimeofInterview.csv')

        #Change the column names as per the table load
        df_final_join = pd.merge(df_states, df_loadcsv, how='inner', left_on='state_code', right_on='State')
        df_final_join['created_datetime'] = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

        #Change with the required column names
        df_drop_cols = df_final_join[['Phase','Indicator','Group','State','Subgroup','Time Period','Time Period Label','Value','Low CI','High CI','Confidence Interval','Quartile range','Supression Flag','created_datetime']]
        df_drop_cols[[ 'Value','Low CI','High CI'  ]] = df_drop_cols[[ 'Value','Low CI','High CI'  ]].fillna(value=0)
        #Change with the required column names
        for i, row in df_drop_cols.iterrows():
                sql = """INSERT INTO public.indicators_of_health_insurance_coverage
(phase, "Indicator", "Group", state, subgroup, time_period, time_period_label, value, low_ci, high_ci, confidence_interval, quartile_range, supression_flag, created_datetime) VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s)"""
                cursor.execute(sql, tuple(row))

        connection.commit()
        connection.close()


def delete_localfile_covid_diagnostic_lab_testing():
        os.remove('/usr/local/airflow/covid_data/COVID19DiagnosticLaboratoryTestingPCRTestingTimeSeries.csv')

def delete_localfile_covid_policy_orders():
        os.remove('/usr/local/airflow/covid_data/COVID19StateandCountyPolicyOrders.csv')

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

def delete_covid_impact_on_hospital_capacity_reported():
        os.remove('/usr/local/airflow/covid_data/COVID19ReportedPatientImpactandHospitalCapacitybyState.csv')
		
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

def delete_monthly_covid_deaths_by_reagion_age_race():
        os.remove('/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageandraceandHispanicorigin.csv')

def delete_monthly_covid_deaths_by_region_age_race_place():
        os.remove('/usr/local/airflow/covid_data/MonthlycountsofCOVID19deathsbyregionageplaceandraceandHispanicorigin.csv')

def delete_indicators_based_on_reported_freq_symptoms():
        os.remove('/usr/local/airflow/covid_data/IndicatorsofAnxietyorDepressionBasedonReportedFrequencyofSymptomsDuringLast7Days.csv')

def delete_indicators_of_health_insurance_coverage():
        os.remove('/usr/local/airflow/covid_data/IndicatorsofHealthInsuranceCoverageattheTimeofInterview.csv')

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

Provisional_Diabetes_Deaths = PythonOperator( task_id='load_Provisional_Diabetes_Deaths', python_callable=load_Provisional_Diabetes_Deaths, dag = dag )

delete_Provisional_Diabetes_Deaths = PythonOperator( task_id='delete_Provisional_Diabetes_Deaths', python_callable=delete_Provisional_Diabetes_Deaths, dag = dag )

Sickle_Cell_Provisional_Deaths = PythonOperator( task_id='load_Sickle_Cell_Provisional_Deaths', python_callable=load_Sickle_Cell_Provisional_Deaths, dag = dag )

delete_Sickle_Cell_Provisional_Deaths = PythonOperator( task_id='delete_Sickle_Cell_Provisional_Deaths', python_callable=delete_Sickle_Cell_Provisional_Deaths, dag = dag )

reimbursement_to_health_care_providers = PythonOperator( task_id='load_reimbursement_to_health_care_providers', python_callable=load_reimbursement_to_health_care_providers, dag = dag )

delete_reimbursement_to_health_care_providers = PythonOperator( task_id='delete_reimbursement_to_health_care_providers', python_callable=delete_reimbursement_to_health_care_providers, dag = dag )

covid_deaths_by_condition_by_agegroup_by_state = PythonOperator( task_id='load_covid_deaths_by_condition_by_agegroup_by_state', python_callable=load_covid_deaths_by_condition_by_agegroup_by_state, dag = dag )

delete_covid_deaths_by_condition_by_agegroup_by_state = PythonOperator( task_id='delete_covid_deaths_by_condition_by_agegroup_by_state', python_callable=delete_covid_deaths_by_condition_by_agegroup_by_state, dag = dag )

covid_case_surveillance_data = PythonOperator( task_id='load_covid_case_surveillance_data', python_callable=load_covid_case_surveillance_data, dag = dag )

delete_covid_case_surveillance_data = PythonOperator( task_id='delete_covid_case_surveillance_data', python_callable=delete_covid_case_surveillance_data, dag = dag )

covid_impact_on_hospital_capacity = PythonOperator( task_id='load_covid_impact_on_hospital_capacity', python_callable=load_covid_impact_on_hospital_capacity, dag = dag )

delete_covid_impact_on_hospital_capacity = PythonOperator( task_id='delete_covid_impact_on_hospital_capacity', python_callable=delete_covid_impact_on_hospital_capacity, dag = dag )

covid_impact_on_hospital_capacity_reported = PythonOperator( task_id='load_covid_impact_on_hospital_capacity_reported', python_callable=load_covid_impact_on_hospital_capacity_reported, dag = dag )

delete_covid_impact_on_hospital_capacity_reported = PythonOperator( task_id='delete_covid_impact_on_hospital_capacity_reported', python_callable=delete_covid_impact_on_hospital_capacity_reported, dag = dag )

covid_deaths_by_county_and_race = PythonOperator( task_id='load_covid_deaths_by_county_and_race', python_callable=load_covid_deaths_by_county_and_race, dag = dag )

delete_covid_deaths_by_county_and_race = PythonOperator( task_id='delete_covid_deaths_by_county_and_race', python_callable=delete_covid_deaths_by_county_and_race, dag = dag )

covid_deaths_by_deathplace_by_state = PythonOperator( task_id='load_covid_deaths_by_deathplace_by_state', python_callable=load_covid_deaths_by_deathplace_by_state, dag = dag )

delete_covid_deaths_by_deathplace_by_state = PythonOperator( task_id='delete_covid_deaths_by_deathplace_by_state', python_callable=delete_covid_deaths_by_deathplace_by_state, dag = dag )

covid_deaths_by_sex_age_by_state = PythonOperator( task_id='load_covid_deaths_by_sex_age_by_state', python_callable=load_covid_deaths_by_sex_age_by_state, dag = dag )

delete_covid_deaths_by_sex_age_by_state = PythonOperator( task_id='delete_covid_deaths_by_sex_age_by_state', python_callable=delete_covid_deaths_by_sex_age_by_state, dag = dag )

covid_deaths_by_sex_age_by_week = PythonOperator( task_id='load_covid_deaths_by_sex_age_by_week', python_callable=load_covid_deaths_by_sex_age_by_week, dag = dag )

delete_covid_deaths_by_sex_age_by_week = PythonOperator( task_id='delete_covid_deaths_by_sex_age_by_week', python_callable=delete_covid_deaths_by_sex_age_by_week, dag = dag )

covid_deaths_by_weekend_by_state = PythonOperator( task_id='load_covid_deaths_by_weekend_by_state', python_callable=load_covid_deaths_by_weekend_by_state, dag = dag )

delete_covid_deaths_by_weekend_by_state = PythonOperator( task_id='delete_covid_deaths_by_weekend_by_state', python_callable=delete_covid_deaths_by_weekend_by_state, dag = dag )

covid_deaths_by_county = PythonOperator( task_id='load_covid_deaths_by_county', python_callable=load_covid_deaths_by_county, dag = dag )

delete_covid_deaths_by_county = PythonOperator( task_id='delete_covid_deaths_by_county', python_callable=delete_covid_deaths_by_county, dag = dag )

covid_deaths_by_age_in_years = PythonOperator( task_id='load_covid_deaths_by_age_in_years', python_callable=load_covid_deaths_by_age_in_years, dag = dag )

delete_covid_deaths_by_age_in_years = PythonOperator( task_id='delete_covid_deaths_by_age_in_years', python_callable=delete_covid_deaths_by_age_in_years, dag = dag )

monthly_covid_deaths_by_reagion_age_race = PythonOperator( task_id='load_monthly_covid_deaths_by_reagion_age_race', python_callable=load_monthly_covid_deaths_by_reagion_age_race, dag = dag )

delete_monthly_covid_deaths_by_reagion_age_race = PythonOperator( task_id='delete_monthly_covid_deaths_by_reagion_age_race', python_callable=delete_monthly_covid_deaths_by_reagion_age_race, dag = dag )

monthly_covid_deaths_by_region_age_race_place = PythonOperator( task_id='load_monthly_covid_deaths_by_region_age_race_place', python_callable=load_monthly_covid_deaths_by_region_age_race_place, dag = dag )

delete_monthly_covid_deaths_by_region_age_race_place = PythonOperator( task_id='delete_monthly_covid_deaths_by_region_age_race_place', python_callable=delete_monthly_covid_deaths_by_region_age_race_place, dag = dag )

indicators_based_on_reported_freq_symptoms = PythonOperator( task_id='load_indicators_based_on_reported_freq_symptoms', python_callable=load_indicators_based_on_reported_freq_symptoms, dag = dag )

delete_indicators_based_on_reported_freq_symptoms = PythonOperator( task_id='delete_indicators_based_on_reported_freq_symptoms', python_callable=delete_indicators_based_on_reported_freq_symptoms, dag = dag )

indicators_of_health_insurance_coverage = PythonOperator( task_id='load_indicators_of_health_insurance_coverage', python_callable=load_indicators_of_health_insurance_coverage, dag = dag )

delete_indicators_of_health_insurance_coverage = PythonOperator( task_id='delete_indicators_of_health_insurance_coverage', python_callable=delete_indicators_of_health_insurance_coverage, dag = dag )

#Setting dependencies between the tasks
fetchurls >> fetch_data_to_local
fetch_data_to_local >> [ covid_diagnostic_lab_testing , covid_policy_orders , Provisional_Covid_Deaths_by_County, Provisional_Diabetes_Deaths , Sickle_Cell_Provisional_Deaths , reimbursement_to_health_care_providers, covid_deaths_by_condition_by_agegroup_by_state, covid_case_surveillance_data , covid_impact_on_hospital_capacity, covid_impact_on_hospital_capacity_reported , covid_deaths_by_county_and_race , covid_deaths_by_deathplace_by_state , covid_deaths_by_sex_age_by_state, covid_deaths_by_sex_age_by_week , covid_deaths_by_weekend_by_state, covid_deaths_by_county , covid_deaths_by_age_in_years , monthly_covid_deaths_by_reagion_age_race, monthly_covid_deaths_by_region_age_race_place, indicators_based_on_reported_freq_symptoms , indicators_of_health_insurance_coverage]
covid_diagnostic_lab_testing >> delete_localfile_covid_diagnostic_lab_testing
covid_policy_orders >> delete_localfile_covid_policy_orders
Provisional_Covid_Deaths_by_County >> delete_Provisional_Covid_Deaths_by_County
Provisional_Diabetes_Deaths >> delete_Provisional_Diabetes_Deaths
Sickle_Cell_Provisional_Deaths >> delete_Sickle_Cell_Provisional_Deaths
reimbursement_to_health_care_providers >> delete_reimbursement_to_health_care_providers
covid_deaths_by_condition_by_agegroup_by_state >> delete_covid_deaths_by_condition_by_agegroup_by_state
covid_case_surveillance_data >> delete_covid_case_surveillance_data
covid_impact_on_hospital_capacity >> delete_covid_impact_on_hospital_capacity
covid_impact_on_hospital_capacity_reported >> delete_covid_impact_on_hospital_capacity_reported
covid_deaths_by_county_and_race >> delete_covid_deaths_by_county_and_race
covid_deaths_by_deathplace_by_state >> delete_covid_deaths_by_deathplace_by_state
covid_deaths_by_sex_age_by_state >> delete_covid_deaths_by_sex_age_by_state
covid_deaths_by_sex_age_by_week >> delete_covid_deaths_by_sex_age_by_week
covid_deaths_by_weekend_by_state >> delete_covid_deaths_by_weekend_by_state
covid_deaths_by_county >> delete_covid_deaths_by_county
covid_deaths_by_age_in_years >> delete_covid_deaths_by_age_in_years
monthly_covid_deaths_by_reagion_age_race >> delete_monthly_covid_deaths_by_reagion_age_race
monthly_covid_deaths_by_region_age_race_place >> delete_monthly_covid_deaths_by_region_age_race_place
indicators_based_on_reported_freq_symptoms >> delete_indicators_based_on_reported_freq_symptoms
indicators_of_health_insurance_coverage >> delete_indicators_of_health_insurance_coverage
