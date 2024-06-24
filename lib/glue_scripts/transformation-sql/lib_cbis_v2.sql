SELECT	day,																				
		month,																		
		year,																																				
		periodname,																			
		organisationunitid,																		
		organisationunitname,																			
		organisationunitcode,																		
		organisationunitdescription,																			
		chss_monthly_service_reports																
FROM liberia.lib_cbis_v2
GROUP BY day, month, year, periodid, periodname, organisationunitid, organisationunitname, organisationunitcode, organisationunitdescription, cchss_monthly_service_reports
ORDER BY day, month, year, periodid, periodname, organisationunitid, organisationunitname, organisationunitcode, organisationunitdescription, chss_monthly_service_reports