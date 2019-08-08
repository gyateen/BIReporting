package com.power2sme.reporting.constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;


import lombok.Data;

@Data
public class ReportingConstants {
	

	
	public static final String PROPERTIES_FILE = "src/main/resources/Reporting.properties";
	
	
	private static final Properties reportingProp = new Properties();

	
	
	
	/*static
	{
		System.out.println(PROPERTIES_FILE);
		InputStream inStream = ReportingConstants.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE);
		if(inStream == null)
			System.out.println("instream is null");
		try {
			reportingProp.load(inStream);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	*/
	
	public static String REPORTING_DIR = "c:\\talend_reports";
	public static String REPORTING_DATE_FORMAT = "yyyy-MM-dd";
	public static String REPORT_ID;
	public static String DB_DRIVER;
	public static String DB_URL;
	public static  String DB_USER;
	public static String DB_PASSWORD;
	public static String DMS_URL = "https://www.power2sme.com/dms/api/v1/uploadfile";
	public static String DMS_DOWNLOAD_URL = "https://www.power2sme.com/dms/api/v1/downloadfile";
	public static String MQ_URL = "https://www.power2sme.com/openbd/mq/endpoint.cfc?";
	public static boolean REPORT_UPDATE = false;
	public static String REPORT_PATH;
	
	
	
	
	public static void init(Properties contextProp)
	{
		
		REPORTING_DIR = contextProp.getProperty("REPORTING_DIR");
		REPORTING_DATE_FORMAT = contextProp.getProperty("REPORTING_DATE_FORMAT");
		REPORT_ID = contextProp.getProperty("REPORT_ID");
		DB_DRIVER = contextProp.getProperty("DB_DRIVER");
		DB_URL = contextProp.getProperty("DB_URL");
		DB_USER = contextProp.getProperty("DB_USER");
		DB_PASSWORD = contextProp.getProperty("DB_PASSWORD");
		REPORT_UPDATE = Boolean.parseBoolean(contextProp.getProperty("REPORT_UPDATE"));
		REPORT_PATH = contextProp.getProperty("REPORT_PATH");
	}
	
	
	

}
