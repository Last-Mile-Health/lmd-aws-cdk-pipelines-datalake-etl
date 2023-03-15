SELECT day(meta_autoDate)                     AS day,
       month(meta_autoDate)                   AS month,
       year(meta_autoDate)                    AS year,
       routineVisitID,
       meta_autoDate,
       chaID,
       visitDate,
       hhID,
       pregnanciesInHousehold,
       numberOfBirthsCommunity,
       maternalDeath
FROM liberia.routineVisit
where (year(meta_autoDate) >= '2015' and year(meta_autoDate) <= '2022')
group by meta_autoDate, year(meta_autoDate), month(meta_autoDate), day(meta_autoDate), routineVisitID, chaID, visitDate, hhID, pregnanciesInHousehold, numberOfBirthsCommunity, maternalDeath
order by meta_autoDate, year(meta_autoDate) desc, month(meta_autoDate), day(meta_autoDate), routineVisitID, chaID, visitDate, hhID, pregnanciesInHousehold, numberOfBirthsCommunity, maternalDeath





