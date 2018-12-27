package com.power2sme.reporting;

import java.util.Properties;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.power2sme.reporting.config.ReportingConfig;
import com.power2sme.reporting.constants.ReportingConstants;
import com.power2sme.reporting.service.ReportingService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReportingJob {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		
		ReportingJob.runJob(getTestContext());
		
		}
	
	public static void runJob(Properties contextProp)
	{
		ReportingConstants.init(contextProp);
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ReportingConfig.class);
		ReportingService reportingService = context.getBean(ReportingService.class);
		log.info("Starting job");
		reportingService.generateAndSendReports();
	}
	
	
	public static Properties getTestContext()
	{
		Properties contextProp = new Properties();
		contextProp.setProperty("DB_DRIVER", "com.mysql.jdbc.Driver");
		contextProp.setProperty("DB_PASSWORD", "^C,qaJ36B");
		contextProp.setProperty("DB_USER", "team_db_wrhouse");
		contextProp.setProperty("DB_URL", "jdbc:mysql://103.25.172.143:3306/p2s_ctrl");
		contextProp.setProperty("REPORTING_DATE_FORMAT", "yyyy-MM-dd");
		contextProp.setProperty("REPORT_ID", "2");
		contextProp.setProperty("REPORTING_DIR", "/home/yateen/talend_reports");
		return contextProp;
	}

}
