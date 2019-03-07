package com.power2sme.etl.constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.power2sme.etl.constants.ETLConstants;

public class ETLConstants {

	private ETLConstants()
	{
		
	}

	public static final String PROPERTIES_FILE = "ETL.properties";
	private static final Properties reportingProp = new Properties();

	static
	{
		InputStream inStream = ETLConstants.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE);
		try {
			reportingProp.load(inStream);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	
	public static final String SRC_DB_DRIVER = reportingProp.getProperty("SRC_DB_DRIVER");
	public static final String SRC_DB_URL = reportingProp.getProperty("SRC_DB_URL");
	public static final String SRC_DB_USER = reportingProp.getProperty("SRC_DB_USER");
	public static final String SRC_DB_PASSWORD = reportingProp.getProperty("SRC_DB_PASSWORD");
	
	public static final String TGT_DB_DRIVER = reportingProp.getProperty("TGT_DB_DRIVER");
	public static final String TGT_DB_URL = reportingProp.getProperty("TGT_DB_URL");
	public static final String TGT_DB_USER = reportingProp.getProperty("TGT_DB_USER");
	public static final String TGT_DB_PASSWORD = reportingProp.getProperty("TGT_DB_PASSWORD");
	public static final String LOG_DATABASE = "ODS_LOG";
	public static final String LOG_DB_URL = reportingProp.getProperty("LOG_DB_URL");
	public static final String LOG_DB_USER = reportingProp.getProperty("LOG_DB_USER");
	public static final String LOG_DB_PASSWORD = reportingProp.getProperty("LOG_DB_PASSWORD");
	public static final String LOG_TABLE = reportingProp.getProperty("LOG_TABLE");
	public static final String LOG_SCHEMA = reportingProp.getProperty("LOG_SCHEMA");
	public static final String INIITAL_JOB_STATUS = reportingProp.getProperty("INIITAL_JOB_STATUS");
	public static final String RULEID_DELIMITER = reportingProp.getProperty("RULEID_DELIMITER");
	public static final String DATE_FORMAT = reportingProp.getProperty("DATE_FORMAT");
	
	public static void init(Properties contextProp) {
		
	}
	
}
