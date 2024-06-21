select
       sickChildFormID,
       meta_autoDate,
       communityID,
       HHID,
       gender,
       visitType,
       coalesce(treatMalaria, 0) AS malaria_cases_treated,
       coalesce(treatDiarrhea, 0) AS diarrhea_cases_treated,
       coalesce(treatPneumonia, 0) AS ari_cases_treated,
       source,
       day,
       month,
       year
from liberia.lib_sick_child
-- where (year(meta_autoDate) >= '2015' and year(meta_autoDate) <= '2022')
group by meta_autoDate, year, month, day, malaria_cases_treated, diarrhea_cases_treated, ari_cases_treated, visitType, gender, HHID, communityID, sickChildFormID, source
order by meta_autoDate, year desc, month, day, malaria_cases_treated, diarrhea_cases_treated, ari_cases_treated, visitType, gender, HHID, communityID, sickChildFormID, source