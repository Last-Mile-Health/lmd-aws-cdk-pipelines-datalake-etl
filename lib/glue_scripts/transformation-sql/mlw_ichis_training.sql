select day(training_date1)     AS day,
       month(training_date1)   AS month,
       year(training_date1)    AS year,
	   number,
	   district,
	   first_name,
	   surname,
	   cast(commonly_used_phone_no as string) as commonly_used_phone_no,
	   gender,
	   age_range,
	   position,
	   reporting_health_facility,
	   hsa_catchment_area,
	   catchment_area_population,
	   catchment_area_setting,
	   attended_ichis_rollout_exercise,
	   cluster_location,
	   training_date1,
	   training_date2,
	   training_date3,
	   training_date4,
	   training_date5,
	   training_date6,
	   training_date7,
	   training_days_attended
from malawi.mlw_ichis_training
group by training_date1, year(training_date1), month(training_date1), day(training_date1), number, district, first_name, surname, commonly_used_phone_no, gender, age_range,position,reporting_health_facility,hsa_catchment_area,catchment_area_population,catchment_area_setting,attended_ichis_rollout_exercise,cluster_location,training_date1,training_date2,training_date3,training_date4,training_date5,training_date6,training_date7,training_days_attended
order by training_date1, year(training_date1) desc, month(training_date1), day(training_date1), number, district, first_name, surname, commonly_used_phone_no, gender, age_range,position,reporting_health_facility,hsa_catchment_area,catchment_area_population,catchment_area_setting,attended_ichis_rollout_exercise,cluster_location,training_date1,training_date2,training_date3,training_date4,training_date5,training_date6,training_date7,training_days_attended

