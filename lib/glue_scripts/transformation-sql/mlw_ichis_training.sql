select day,
       month,
       year,
	   district,
	   reporting_health_facility_name,
	   full_name_of_participant,
	   phone_number,
	   age,
	   gender,
	   training_id_number,
	   catchment_area_population_size,
	   years_worked_as_a_health_worker,
	   position,
	   education,
	   trained_in_ichis_before,
	   fy,
	   quarter,
	   community_register_module,
	   household_register_module,
	   person_register_module,
	   epi_module,
	   imci_module,
	   eldsr_module,
	   cbmnc_module,
	   family_planning_module,
	   reporting_module
from malawi.mlw_ichis_training
group by day, month, year, number, district, reporting_health_facility_name, full_name_of_participant,phone_number, age, gender, training_id_number, catchment_area_population_size, years_worked_as_a_health_worker, position, education, trained_in_ichis_before, fy, quarter, community_register_module, household_register_module, person_register_module, epi_module, imci_module, eldsr_module, cbmnc_module,	family_planning_module,	reporting_module
order by day, month, year, number, district, reporting_health_facility_name, full_name_of_participant,phone_number, age, gender, training_id_number, catchment_area_population_size, years_worked_as_a_health_worker, position, education, trained_in_ichis_before, fy, quarter, community_register_module, household_register_module, person_register_module, epi_module, imci_module, eldsr_module, cbmnc_module,	family_planning_module,	reporting_module


