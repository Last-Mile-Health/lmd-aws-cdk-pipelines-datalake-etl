select day(meta_autoDate)     AS day,
       month(meta_autoDate)   AS month,
       year(meta_autoDate)    AS year,
       sickChildFormID,
       meta_autoDate,
       communityID,
       HHID,
       gender,
       visitType,
       coalesce(treatMalaria, 0) AS malaria_cases_treated,
       coalesce(treatDiarrhea, 0) AS diarrhea_cases_treated,
       coalesce(treatPneumonia, 0) AS ari_cases_treated,
       source
from liberia.lib_sick_child
where (year(meta_autoDate) >= '2015' and year(meta_autoDate) <= '2022')
group by meta_autoDate, year(meta_autoDate), month(meta_autoDate), day(meta_autoDate), malaria_cases_treated, diarrhea_cases_treated, ari_cases_treated, visitType, gender, HHID, communityID, sickChildFormID, source
order by meta_autoDate, year(meta_autoDate) desc, month(meta_autoDate), day(meta_autoDate), malaria_cases_treated, diarrhea_cases_treated, ari_cases_treated, visitType, gender, HHID, communityID, sickChildFormID, source