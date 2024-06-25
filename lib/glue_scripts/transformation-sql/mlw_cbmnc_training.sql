SELECT	day,																				
		month,																		
		year,																																				
		Participant Name,
		Phonenumber,
		Age,
		Gender ,
		Position,
		Education,
		District,
		Health facility,
		Catchmentarea
																
FROM malawi.mlw_cmbnc_training
GROUP BY day, month, year, Participant Name, Phonenumber, Age, Gender , Position, Education, District, Health facility, Catchmentarea
ORDER BY day, month, year, Participant Name, Phonenumber, Age, Gender , Position, Education, District, Health facility, Catchmentarea