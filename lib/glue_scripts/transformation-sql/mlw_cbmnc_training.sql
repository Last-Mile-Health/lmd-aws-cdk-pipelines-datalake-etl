SeLecT	day,																				
		month,																		
		year,																																				
		participant_name,
		phonenumber,
		age,
		gender ,
		position,
		education,
		district,
		health facility,
		catchmentarea
																
FROM malawi.mlw_cmbnc_training
GROUp BY day, month, year, participant_name, phonenumber, age, gender , position, education, district, health_facility, catchmentarea
ORdeR BY day, month, year, participant_name, phonenumber, age, gender , position, education, district, health_facility, catchmentarea