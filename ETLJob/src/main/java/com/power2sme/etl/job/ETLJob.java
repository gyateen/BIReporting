package com.power2sme.etl.job;

import java.util.Properties;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.power2sme.etl.config.ETLConfig;
import com.power2sme.etl.constants.ETLConstants;
import com.power2sme.etl.service.ETLService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETLJob {

	
	public static void runJob(Properties contextProp)
	{
		ETLConstants.init(contextProp);
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ETLConfig.class);
		ETLService etlService = context.getBean(ETLService.class);
		log.info("Starting job");
		String srcSchema = contextProp.getProperty("SRC_SCHEMA");
		String tgtSchema = contextProp.getProperty("TGT_SCHEMA");
		String tgtUser = contextProp.getProperty("TGT_USER");
		String srcUser = contextProp.getProperty("SRC_USER");
		String srcQuery = contextProp.getProperty("QRY");
		String tgtTable = contextProp.getProperty("TGT_TBL");
		
		
		etlService.getETLQuery(srcSchema, srcQuery, tgtSchema, tgtTable, null);
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {

		ETLJob.runJob(getTestContext());
	}
	
	public static Properties getTestContext()
	{
		Properties contextProp = new Properties();
		contextProp.setProperty("TGT_DRIVER", "com.mysql.jdbc.Driver");
		contextProp.setProperty("SRC_DRIVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
		contextProp.setProperty("TGT_DB_PASSWORD", "^C,qaJ36B");
		contextProp.setProperty("TGT_USER", "team_db_wrhouse");
		contextProp.setProperty("TGT_DB_URL", "jdbc:mysql://192.168.1.14:3306/p2s_ctrl");
		contextProp.setProperty("SRC_DB_PASSWORD", "zsa!123");
		contextProp.setProperty("SRC_USER", "shweta");
		contextProp.setProperty("SRC_DB_URL", "jdbc:sqlserver://103.25.172.167:1433;");
		contextProp.setProperty("REPORTING_DATE_FORMAT", "yyyy-MM-dd");
		contextProp.setProperty("JOB_ID", "2");
		contextProp.setProperty("QRY", "select * from bs_nav_cluster_details")
		contextProp.setProperty("TGT_SCHEMA", "p2s_bs");
		contextProp.setProperty("TGT_TBL", "bs_nav_cluster_details");
		return contextProp;
	}
	
}
	
