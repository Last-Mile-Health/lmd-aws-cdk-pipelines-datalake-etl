SELECT
    start_date,
    end_date,
    id_number,
    full_name,
    phone_number,
    health_facility,
    catchment_area,
    age,
    gender,
    position,
    work_location,
    highest_level_of_education,
    district,
    reporting_health_facility_name,
    reporting_catchment_area_name,
    total_score	pre_assessment_score,
    test_type,			
    day,
    month,
    year
FROM malawi.mlw_cbmnc_training
GROUP BY start_date, end_date, id_number, full_name, phone_number, health_facility, catchment_area, age, gender, position, work_location, highest_level_of_education, district, reporting_health_facility_name, reporting_catchment_area_name, total_score, pre_assessment_score, test_type, status, day, month, year
ORDER BY start_date, end_date, id_number, full_name, phone_number, health_facility, catchment_area, age, gender, position, work_location, highest_level_of_education, district, reporting_health_facility_name, reporting_catchment_area_name, total_score, pre_assessment_score, test_type, status, day, month, year