drop table if exists states_r;

create table states_r ( state_key serial primary key , state_code varchar(10) not null unique, state_name varchar(50) not null unique, created_datetime timestamp not null);

drop table if exists calendar_r;

create table calendar_r (calendar_key serial primary key, calendar_date date not null unique, calendar_month int not null, calendar_year int not null, week_start_date date not null, week_end_date date not null, created_datetime timestamp not null);

drop table if exists project_audit_table;

create table project_audit_table ( table_key int primary key, table_name varchar(100), pickup_criteria date, run_date date );

drop table if exists Provisional_Covid_Deaths_by_County;

create table Provisional_Covid_Deaths_by_County ( week_start_calendar_key int, week_end_calendar_key int, state_key int, County varchar(100), Fips_Code int, COVID_19_Deaths int, Total_Deaths int , Created_Datetime timestamp not null, constraint fk_covid_deaths_by_county1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county3 foreign key(state_key) references states_r(state_key) );

drop table if exists Provisional_Diabetes_Deaths;

create table Provisional_Diabetes_Deaths( calendar_key int, Date_Of_Death_Year int, Date_Of_Death_Month int, AgeGroup varchar(20), Sex varchar(20), COVID19 int, Diabetes_uc int, Diabetes_mc int, C19PlusDiabetes int, C19PlusHypertensiveDiseases int, C19PlusMajorCardiovascularDiseases int, C19PlusHypertensiveDiseasesAndMCVD int, C19PlusChronicLowerRespiratoryDisease int, C19PlusKidneyDisease int, C19PlusChronicLiverDiseaseAndCirrhosis int, C19PlusObesity int, created_datetime timestamp not null , constraint fk_Provisional_Diabetes_Deaths1 foreign key(calendar_key) references calendar_r(calendar_key) );

drop table if exists Sickle_Cell_Provisional_Deaths;

create table Sickle_Cell_Provisional_Deaths ( calendar_key int, Date_Of_Death_Year int, Quarter int, Race_or_Hispanic_Origin varchar(50), Age_Group varchar(50), SCD_Underlying int, SCD_Multi int, SCD_and_COVID_19 int, created_datetime timestamp not null , constraint fk_Sickle_Cell_Provisional_Deaths foreign key(calendar_key) references calendar_r(calendar_key) );

drop table if exists Reimbursement_to_Health_Care_Providers;

create table Reimbursement_to_Health_Care_Providers ( Provider_Name varchar(100) not null, state_key int, City varchar(100), Claims_Paid_for_Testing varchar(100), Claims_Paid_for_Treatment varchar(100), Georeferenced_Column varchar(100), created_datetime timestamp not null , constraint fk_Reimbursement_to_Health_Care_Providers foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_condition_by_agegroup_by_state;

create table covid_deaths_by_condition_by_agegroup_by_state ( 
week_start_calendar_key int, 
week_end_calendar_key int, 
state_key int,
date_as_of date, 
condition_group varchar(200), 
condition varchar(200), 
ICD10_codes varchar(200), 
Age_group varchar(50), 
Number_of_COVID_19_Deaths int, 
flag varchar(200), 
created_datetime timestamp not null , constraint fk_covid_deaths_by_condition_by_agegroup_by_state1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_condition_by_agegroup_by_state2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_condition_by_agegroup_by_state3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_case_surveillance_data;

create table covid_case_surveillance_data ( cdc_report_key int, pos_spec_key int, onset_dt varchar(30), current_status varchar(200), sex varchar(20), age_group varchar(200), Race_and_ethnicity varchar(200), hosp_yn varchar(50), icu_yn varchar(50), death_yn varchar(50), medcond_yn varchar(50), created_datetime timestamp not null , constraint fk_covid_case_surveillance_data1 foreign key(cdc_report_key) references calendar_r(calendar_key) , constraint fk_covid_case_surveillance_data2 foreign key(pos_spec_key) references calendar_r(calendar_key) );

drop table if exists covid_diagnostic_lab_testing;

create table covid_diagnostic_lab_testing ( state_key int, calendar_key int, state_fips int, fema_region varchar(50), overall_outcome varchar(50), new_results_reported int, total_results_reported int, created_datetime timestamp not null , constraint fk_covid_diagnostic_lab_testing1 foreign key(calendar_key) references calendar_r(calendar_key) , constraint fk_covid_diagnostic_lab_testing2 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_impact_on_hospital_capacity;

create table covid_impact_on_hospital_capacity ( state_key int, calendar_key int, Inpatient_Beds_Occupied_Estimated varchar(20), Count_LL varchar(20), Count_UL varchar(20), Percentage_of_Inpatient_Beds_Occupied_Estimated float, Percentage_LL float, Percentage_UL float, Total_Inpatient_Beds varchar(20), Total_LL varchar(20), Total_UL varchar(20), created_datetime timestamp not null , constraint fk_covid_impact_on_hospital_capacity1 foreign key(calendar_key) references calendar_r(calendar_key) , constraint fk_covid_impact_on_hospital_capacity2 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_impact_on_hospital_capacity_reported;

create table covid_impact_on_hospital_capacity_reported ( state_key int, hospital_onset_covid int, hospital_onset_covid_coverage int, inpatient_beds int, inpatient_beds_coverage int, inpatient_beds_used int, inpatient_beds_used_coverage int, inpatient_beds_used_covid int, inpatient_beds_used_covid_coverage int, staffed_adult_icu_bed_occupancy int, staffed_adult_icu_bed_occupancy_coverage int, staffed_icu_adult_patients_confirmed_and_suspected_covid int, staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage int, staffed_icu_adult_patients_confirmed_covid int, staffed_icu_adult_patients_confirmed_covid_coverage int, total_adult_patients_hospitalized_confirmed_and_suspected_covid int, total_adult_patients_hospitalized_confirmed_covid int, total_adult_patients_hospitalized_confirmed_covid_coverage int, total_pediatric_patients_hospitalized_confirmed_and_suspected_covid int, total_pediatric_patients_hospitalized_confirmed_covid int, total_pediatric_patients_hospitalized_confirmed_covid_coverage int, total_staffed_adult_icu_beds int, total_staffed_adult_icu_beds_coverage int, inpatient_beds_utilization float, inpatient_beds_utilization_coverage int, inpatient_beds_utilization_numerator int, inpatient_beds_utilization_denominator int, percent_of_inpatients_with_covid float, percent_of_inpatients_with_covid_coverage int, percent_of_inpatients_with_covid_numerator int, percent_of_inpatients_with_covid_denominator int, inpatient_bed_covid_utilization float, inpatient_bed_covid_utilization_coverage int, inpatient_bed_covid_utilization_numerator int, inpatient_bed_covid_utilization_denominator int, adult_icu_bed_covid_utilization float, adult_icu_bed_covid_utilization_coverage int, adult_icu_bed_covid_utilization_numerator int, adult_icu_bed_covid_utilization_denominator int, adult_icu_bed_utilization float, adult_icu_bed_utilization_coverage int, adult_icu_bed_utilization_numerator int, adult_icu_bed_utilization_denominator int, reporting_cutoff_start date, created_datetime timestamp not null, constraint fk_covid_impact_on_hospital_capacity_reported foreign key(state_key) references states_r(state_key) );

drop table if exists covid_policy_orders;

create table covid_policy_orders ( state_key int, calendar_key int, county varchar(50), fips_code varchar(100), policy_level varchar(100), policy_type varchar(2000), start_stop varchar(200), comments varchar(10000), source varchar(10000), total_phases varchar(50), created_datetime timestamp not null, constraint fk_covid_policy_orders1 foreign key(calendar_key) references calendar_r(calendar_key) , constraint fk_covid_policy_orders2 foreign key(state_key) references states_r(state_key) );

drop table if exists deaths_by_jurisdiction_race_Hispanic_weekly;

create table deaths_by_jurisdiction_race_Hispanic_weekly ( state_key int, 
week_ending_date date, 
Race_Ethnicity varchar(100), 
Time_Period varchar(100), 
Suppress varchar(1000), 
Note varchar(10000), 
Outcome varchar(1000), 
Number_of_Deaths int, 
Average_Number_of_Deaths_in_Time_Period int, 
Difference_from_2015_2019_to_2020 int, 
Percent_Difference_from_2015_2019_to_2020 float, 
"Type" varchar(100) , 
created_datetime timestamp not null, 
constraint fk_deaths_by_jurisdiction_race_Hispanic_weekly1 
foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_county_and_race;

create table covid_deaths_by_county_and_race ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, County_Name varchar(100), Urban_Rural_Code int, FIPS_State int, FIPS_County int, FIPS_Code int, indicator varchar(100), Total_deaths int, COVID_19_Deaths int, Non_Hispanic_White float, Non_Hispanic_Black float, Non_Hispanic_American_Indian_or_Alaska_Native float, Non_Hispanic_Asian float, Other float, Hispanic float, Urban_Rural_Description varchar(100), Footnote varchar(100), created_datetime timestamp not null, constraint fk_covid_deaths_by_county_and_race1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county_and_race2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county_and_race3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_deathplace_by_state;

create table covid_deaths_by_deathplace_by_state ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, Place_of_Death varchar(100), COVID19_Deaths bigint, Total_Deaths bigint, Pneumonia_Deaths bigint, Pneumonia_and_COVID19_Deaths bigint, Influenza_Deaths bigint, Pneumonia_Influenza_or_COVID19_Deaths bigint, Footnote varchar(1000), created_datetime timestamp, constraint fk_covid_deaths_by_deathplace_by_state1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_deathplace_by_state2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_deathplace_by_state3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_sex_age_by_state;

create table covid_deaths_by_sex_age_by_state ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, Sex varchar(100), Age_Group varchar(100), COVID19_Deaths int, Total_Deaths int, Pneumonia_Deaths int, Pneumonia_and_COVID19_Deaths int, Influenza_Deaths int, Pneumonia_Influenza_or_COVID19_Deaths int, Footnote varchar(1000), created_datetime timestamp, constraint fk_covid_deaths_by_sex_age_by_state1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_sex_age_by_state2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_sex_age_by_state3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_sex_age_by_week;

create table covid_deaths_by_sex_age_by_week ( week_end_calendar_key int, state_key int, Data_as_of date, Sex varchar(100), Age_Group varchar(100), COVID19_Deaths int, Total_Deaths int, created_datetime timestamp, constraint fk_covid_deaths_by_sex_age_by_week1 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_sex_age_by_week2 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_weekend_by_state;

create table covid_deaths_by_weekend_by_state ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, "group" varchar(100), "Indicator" varchar(100), COVID19_Deaths bigint, Total_Deaths bigint, Pneumonia_Deaths bigint, Pneumonia_and_COVID19_Deaths bigint, Influenza_Deaths bigint, Pneumonia_Influenza_or_COVID19_Deaths bigint, Footnote varchar(1000), created_datetime timestamp, constraint fk_covid_deaths_by_weekend_by_state1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_weekend_by_state2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_weekend_by_state3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_county;

create table covid_deaths_by_county ( week_start_calendar_key int, week_end_calendar_key int, state_key int, data_as_of date, county_name varchar(100), fips_county int, deaths_involving_covid_19 int, deaths_from_all_causes int, created_datetime timestamp, constraint fk_covid_deaths_by_county1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_county3 foreign key(state_key) references states_r(state_key) );

drop table if exists covid_deaths_by_age_in_years;

create table covid_deaths_by_age_in_years ( week_start_calendar_key int, week_end_calendar_key int, Data_as_of date, Sex varchar(100), Age_Years varchar(100), Total_Deaths int, COVID19_Deaths int, created_datetime timestamp, constraint fk_covid_deaths_by_age_in_years1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_covid_deaths_by_age_in_years2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) );

drop table if exists monthly_covid_deaths_by_reagion_age_race;

create table monthly_covid_deaths_by_reagion_age_race ( Data_as_of date, Date_Of_Death_Year int, Date_Of_Death_Month varchar(100), Region varchar(100), AgeGroup varchar(100), RaceEthnicity varchar(100), COVID_19_U071_Multiple_Cause_of_Death int, Sex varchar(100), Place_Of_Death varchar(100), Note1 varchar(1000), Note2 varchar(1000), created_datetime timestamp );

drop table if exists monthly_covid_deaths_by_region_age_race_place;

create table monthly_covid_deaths_by_region_age_race_place ( AnalysisDate date not null, Date_Of_Death_Year int, Date_Of_Death_Month varchar(100), Region varchar(100), AgeGroup varchar(100), RaceEthnicity varchar(100), COVID_19_U071_Multiple_Cause_of_Death int, Sex varchar(100), Place_Of_Death varchar(100), Note varchar(1000), flag_cov19mcod varchar(1000), created_datetime timestamp not null );

drop table if exists indicators_based_on_reported_freq_symptoms;

create table indicators_based_on_reported_freq_symptoms ( state_key int, Phase int, "Indicator" varchar(100), "Group" varchar(100), Subgroup varchar(100), Time_Period int, Time_Period_Label varchar(100), Value float, Low_CI float, High_CI float, Confidence_Interval varchar(100), Quartile_range varchar(100), created_datetime timestamp, constraint fk_indicators_based_on_reported_freq_symptoms foreign key(state_key) references states_r(state_key) );

drop table if exists indicators_of_health_insurance_coverage;

create table indicators_of_health_insurance_coverage ( state_key int, Phase int, "Indicator" varchar(100), "Group" varchar(100), Subgroup varchar(100), Time_Period int, Time_Period_Label varchar(100), Value float, Low_CI float, High_CI float, Confidence_Interval varchar(100), Quartile_range varchar(100), supression_flag int, created_datetime timestamp, constraint fk_indicators_of_health_insurance_coverage foreign key(state_key) references states_r(state_key) );

drop table if exists indicators_reduced_access_care_dueto_covid;

create table indicators_reduced_access_care_dueto_covid ( state_key int, Phase int, "Indicator" varchar(100), "Group" varchar(100), State varchar(100), Subgroup varchar(100), "week" int, week_label varchar(100), Value float, Low_CI float, High_CI float, Confidence_Interval varchar(100), Quartile_range varchar(100), created_datetime timestamp, constraint fk_indicators_of_health_insurance_coverage foreign key(state_key) references states_r(state_key) );

drop table if exists loss_of_work_due_to_illness;

create table loss_of_work_due_to_illness ( "Round" int, "Indicator" varchar(100), "Group" varchar(100), Subgroup varchar(100), sample_size int, "percent" float, standard_error float, supression float, significant float, created_datetime timestamp );

drop table if exists mental_healthcare;

create table mental_healthcare ( state_key int, 
Phase int, 
"Indicator" varchar(100), 
"Group" varchar(100), 
Subgroup varchar(100), 
Time_Period int, 
Time_Period_Label varchar(100), 
Value float, 
Low_CI float, 
High_CI float, 
Confidence_Interval varchar(100), 
Quartile_range varchar(100), 
supression_flag int, 
created_datetime timestamp,
constraint fk_indicators_of_health_insurance_coverage foreign key(state_key) references states_r(state_key) );

drop table if exists Provisional_Deaths_coronavirus;

create table Provisional_Deaths_coronavirus ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, "Group" varchar(100), "Indicator" varchar(100), All_COVID_19_Deaths_U07_1 int, Deaths_from_All_Causes int, Percent_of_Expected_Deaths int, All_Pneumonia_Deaths_J12_0_J18_9 int, Deaths_with_Pneumonia_and_COVID_19_J120_J189_and_U071 int, All_Influenza_Deaths_J09_J11 int, Pneumonia_Influenza_and_COVID_19_Deaths int, Footnote varchar(1000), created_datetime timestamp, constraint fk_Provisional_Deaths_coronavirus1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_Provisional_Deaths_coronavirusy2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_Provisional_Deaths_coronavirus3 foreign key(state_key) references states_r(state_key) );

drop table if exists Provisional_Death_Counts_Influenza_Pneumonia_and_COVID;

create table Provisional_Death_Counts_Influenza_Pneumonia_and_COVID ( week_start_calendar_key int, week_end_calendar_key int, Data_As_Of date, MMWRyear int, MMWRweek int, "Group" varchar(100), "Indicator" varchar(100), Jurisdiction varchar(100), Age_Group varchar(100), COVID_19_Deaths int, Total_Deaths int, Pneumonia_Deaths int, Influenza_Deaths int, Pneumonia_or_Influenza int, Pneumonia_Influenza_OR_COVID_19_Deaths int, Footnote varchar(1000), created_datetime timestamp, constraint fk_Provisional_Death_Counts_Influenza_Pneumonia_and_COVID1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_Provisional_Death_Counts_Influenza_Pneumonia_and_COVID2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) );

drop table Reduced_Access_to_Care_RANDS_during_COVID;

create table Reduced_Access_to_Care_RANDS_during_COVID ( "Round" int, "Indicator" varchar(100), "Group" varchar(100), Subgroup varchar(100), Sample_Size int, Response varchar(100), "Percent" float, Standard_Error float, Suppression varchar(20), Significant int, created_datetime timestamp );

drop table Telemedicine_RANDS_during_COVID19;

create table Telemedicine_RANDS_during_COVID19 ( "Round" int, "Indicator" varchar(100), "Group" varchar(100), Subgroup varchar(100), Sample_Size int, Response varchar(100), "Percent" float, Standard_Error float, Suppression varchar(100), Significant int, created_datetime timestamp );

drop table if exists UnitedStates_COVID19_Cases_Deaths_by_State_overTime;

create table UnitedStates_COVID19_Cases_Deaths_by_State_overTime ( 
state_key int, calendar_key int, submission_date date, state varchar(20), tot_cases int, 
conf_cases int, prob_cases int, new_case int, pnew_case int, tot_death int, conf_death int, 
prob_death int, new_death int, pnew_death int, created_at timestamp, consent_cases varchar(20), 
consent_deaths varchar(20), created_datetime timestamp, 
constraint fk_UnitedStates_COVID19_Cases_Deaths_by_State_overTime1 
foreign key(calendar_key) references calendar_r(calendar_key) , 
constraint fk_UnitedStates_COVID19_Cases_Deaths_by_State_overTime2 
foreign key(state_key) references states_r(state_key) );

drop table if exists US_Stateand_Territorial_Stay_AtHome_Order;

create table US_Stateand_Territorial_Stay_AtHome_Order ( state_key int, County_Name varchar(100), FIPS_State int, FIPS_County int, date date, Current_order_status varchar(100), Order_code int, Issuing_Jurisdiction varchar(100), Stay_at_Home_Order_Recommendation varchar(100), Effective_date date, Expiration_date date, Effective_NA_Reason date, Expiration_NA_Reason varchar(100), Date_Signed date, Express_Preemption varchar(100), origin_dataset varchar(100), County varchar(100), Source_of_Action varchar(100), URL varchar(500), Citation varchar(300), flag int, created_datetime timestamp, constraint fk_US_StateandTerritorialStayAtHomeOrder1 foreign key(state_key) references states_r(state_key) );

drop table counts_of_deaths_by_jurisdiction_and_race_and_Hispanic;

create table counts_of_deaths_by_jurisdiction_and_race_and_Hispanic ( state_key int, Week_Ending_Date date, MMWRYear int, MMWRWeek int, Race_Ethnicity varchar(100), Time_Period varchar(20), Suppress varchar(1000), Note varchar(1000), Outcome varchar(1000), Number_of_Deaths float, Average_Number_of_Deaths_in_Time_Period float, Difference_from_2015_2019_to_2020 float, Percent_Difference_from_2015_2019_to_2020 float, "Type" varchar(1000), created_datetime timestamp, constraint fk_counts_of_deaths_by_jurisdiction_and_race_and_Hispanic foreign key(state_key) references states_r(state_key) );

drop table if exists US_Stateand_Territorial_Stay_AtHome_Order1;

create table US_Stateand_Territorial_Stay_AtHome_Order1 ( state_key int, County_Name varchar(100), FIPS_State int, FIPS_County int, date date, Current_order_status varchar(100), Order_code int, Issuing_Jurisdiction varchar(100), Stay_at_Home_Order_Recommendation varchar(100), Effective_date date, Expiration_date date, Effective_NA_Reason date, Expiration_NA_Reason varchar(100), Date_Signed date, Express_Preemption varchar(100), origin_dataset varchar(100), County varchar(100), Source_of_Action varchar(100), URL varchar(500), Citation varchar(300), flag int, created_datetime timestamp, constraint fk_US_StateandTerritorialStayAtHomeOrder11 foreign key(state_key) references states_r(state_key) );

drop table if exists Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age;

create table Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age ( week_start_calendar_key int, week_end_calendar_key int, Data_as_of date, Sex varchar(20), Race_Ethnicity varchar(100), Age_group varchar(100), "Year" int, MMWRWeek int, AllCause int, NaturalCause int, Septicemia int, Malignant_neoplasms int, Diabetes_mellitus int, Alzheimer_disease int, Influenza_and_pneumonia int, Chronic_lower_respiratory_diseases int, Other_diseases_of_respiratory_system int, Nephritis__nephrotic_syndrome_and_nephrosis int, Symptoms_signs_and_abnormal_clinical_and_laboratory_findings_not_elsewhere_classified int, Diseases_of_heart int, Cerebrovascular_diseases int, COVID_19_U071_Multiple_Cause_of_Death int, COVID_19_U071_Underlying_Cause_of_Death int, created_datetime timestamp, constraint fk_Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) );

drop table if exists Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age_7_4_2020;

create table Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age_7_4_2020 ( week_start_calendar_key int, week_end_calendar_key int, Data_as_of date, Sex varchar(20), Race_Ethnicity varchar(100), Age_group varchar(100), "Year" int, MMWRWeek int, AllCause int, NaturalCause int, Septicemia int, Malignant_neoplasms int, Diabetes_mellitus int, Alzheimer_disease int, Influenza_and_pneumonia int, Chronic_lower_respiratory_diseases int, Other_diseases_of_respiratory_system int, Nephritis__nephrotic_syndrome_and_nephrosis int, Symptoms_signs_and_abnormal_clinical_and_laboratory_findings_not_elsewhere_classified int, Diseases_of_heart int, Cerebrovascular_diseases int, COVID_19_U071_Multiple_Cause_of_Death int, COVID_19_U071_Underlying_Cause_of_Death int, created_datetime timestamp, constraint fk_Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age_1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_Cumulative_Provisional_Countsof_Deathsby_Sex_Race_Age_2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) );

drop table if exists deaths_involving_covid_by_race_hispanic_group_age_by_state;

create table deaths_involving_covid_by_race_hispanic_group_age_by_state ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, Age_group varchar(100), Race_and_Hispanic_Origin_Group varchar(100), COVID19_Deaths int, Total_Deaths int, Pneumonia_Deaths int, Pneumonia_and_COVID19_Deaths int, Influenza_Deaths int, Pneumonia_Influenza_or_COVID19_Deaths int, Footnote varchar(1000), created_datetime timestamp, constraint fk_deaths_involving_covid_by_race_hispanic_age_by_state1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_deaths_involving_covid_by_race_hispanic_oage_by_state2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_deaths_involving_covid_by_race_hispanic_age_by_state3 foreign key(state_key) references states_r(state_key) );

drop table if exists distribution_of_covid_deaths_by_juristriction_by_age;

create table distribution_of_covid_deaths_by_juristriction_by_age ( week_start_calendar_key int, week_end_calendar_key int, state_key int, Data_as_of date, Race_Hispanic_origin varchar(1000), Count_of_COVID19_deaths int, Distribution_of_COVID19_deaths float, Unweighted_distribution_of_population float, Weighted_distribution_of_population float, Difference_between_COVID19_and_unweighted_population float, Difference_between_COVID19_and_weighted_population float, AgeGroup varchar(1000), Suppression varchar(1000), created_datetime timestamp, constraint fk_distribution_of_covid_deaths_by_juristriction_by_age1 foreign key(week_start_calendar_key) references calendar_r(calendar_key) , constraint fk_distribution_of_covid_deaths_by_juristriction_by_age2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_distribution_of_covid_deaths_by_juristriction_by_age3 foreign key(state_key) references states_r(state_key) );

drop table if exists excess_deaths_associated_with_covid;

create table excess_deaths_associated_with_covid ( week_end_calendar_key int, state_key int, Observed_Number int, Upper_Bound_Threshold int, Exceeds_Threshold varchar(100), Average_Expected_Count int, Excess_Lower_Estimate int, Excess_Higher_Estimate int, "Year" int, Total_Excess_Lower_Estimate_in_2020 int, Total_Excess_Higher_Estimate_in_2020 int, Percent_Excess_Lower_Estimate float, Percent_Excess_Higher_Estimate float, "Type" varchar(100), Outcome varchar(100), Suppress varchar(100), Note varchar(1000), Created_Datetime timestamp not null, constraint fk_excess_deaths_associated_with_covid2 foreign key(week_end_calendar_key) references calendar_r(calendar_key) , constraint fk_excess_deaths_associated_with_covid3 foreign key(state_key) references states_r(state_key) );

insert
	into
	project_audit_table (table_key, table_name, pickup_criteria, run_date)
values (1, 'counts_of_deaths_by_jurisdiction_and_race_and_hispanic', '1900-01-01', '1900-01-01') ,
(2, 'covid_case_surveillance_data', '1900-01-01', '1900-01-01') ,
(4, 'covid_deaths_by_condition_by_agegroup_by_state', '1900-01-01', '1900-01-01') ,
(9, 'covid_deaths_by_sex_age_by_week', '1900-01-01', '1900-01-01') ,
(12, 'covid_impact_on_hospital_capacity', '1900-01-01', '1900-01-01') ,
(13, 'covid_impact_on_hospital_capacity_reported', '1900-01-01', '1900-01-01') ,
(16, 'deaths_by_jurisdiction_race_hispanic_weekly', '1900-01-01', '1900-01-01') ,
(17, 'deaths_involving_covid_by_race_hispanic_group_age_by_state', '1900-01-01', '1900-01-01') ,
(18, 'distribution_of_covid_deaths_by_juristriction_by_age', '1900-01-01', '1900-01-01') ,
(19, 'excess_deaths_associated_with_covid', '1900-01-01', '1900-01-01') ;

insert
	into
	project_audit_table (table_key, table_name, pickup_criteria, run_date)
values (20, 'indicators_based_on_reported_freq_symptoms', '1900-01-01', '1900-01-01') ,
(21, 'indicators_of_health_insurance_coverage', '1900-01-01', '1900-01-01') ,
(22, 'indicators_reduced_access_care_dueto_covid', '1900-01-01', '1900-01-01') ,
(23, 'mental_healthcare', '1900-01-01', '1900-01-01') ,
(24, 'loss_of_work_due_to_illness', '1900-01-01', '1900-01-01') ,
(28, 'provisional_death_counts_influenza_pneumonia_and_covid', '1900-01-01', '1900-01-01') ,
(29, 'provisional_deaths_coronavirus', '1900-01-01', '1900-01-01') ,
(31, 'reduced_access_to_care_rands_during_covid', '1900-01-01', '1900-01-01') ,
(32, 'reimbursement_to_health_care_providers', '1900-01-01', '1900-01-01') ,
(34, 'telemedicine_rands_during_covid19', '1900-01-01', '1900-01-01') ;

insert
	into
	project_audit_table (table_key, table_name, pickup_criteria, run_date)
values (35, 'unitedstates_covid19_cases_deaths_by_state_overtime', '1900-01-01', '1900-01-01') ,
(36, 'us_stateand_territorial_stay_athome_order', '1900-01-01', '1900-01-01') ,
(37, 'us_stateand_territorial_stay_athome_order1', '1900-01-01', '1900-01-01') ,
(26, 'monthly_covid_deaths_by_region_age_race_place', '1900-01-01', '2020-11-02') ,
(25, 'monthly_covid_deaths_by_reagion_age_race', '1900-01-01', '2020-11-02') ,
(15, 'cumulative_provisional_countsof_deathsby_sex_race_age_7_4_2020', '1900-01-01', '2020-11-02') ,
(14, 'cumulative_provisional_countsof_deathsby_sex_race_age', '1900-01-01', '2020-11-02') ,
(7, 'covid_deaths_by_deathplace_by_state', '1900-01-01', '2020-11-02') ,
(10, 'covid_deaths_by_weekend_by_state', '1900-01-01', '2020-11-02') ,
(8, 'covid_deaths_by_sex_age_by_state', '1900-01-01', '2020-11-02') ;

insert
	into
	project_audit_table (table_key, table_name, pickup_criteria, run_date)
values (6, 'covid_deaths_by_county_and_race', '1900-01-01', '2020-11-02') ,
(3, 'covid_deaths_by_age_in_years', '1900-01-01', '2020-11-02') ,
(5, 'covid_deaths_by_county', '1900-01-01', '2020-11-02') ,
(33, 'sickle_cell_provisional_deaths', '1900-01-01', '2020-09-21') ,
(30, 'provisional_diabetes_deaths', '1900-01-01', '2020-10-20') ,
(27, 'provisional_covid_deaths_by_county', '1900-01-01', '2020-01-08') ,
(11, 'covid_diagnostic_lab_testing', '1900-01-01', '2020-10-29') ,
(38, 'covid_policy_orders', '1900-01-01', '1900-01-01') ;

