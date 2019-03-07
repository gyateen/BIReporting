package com.power2sme.etl.config;

public class DataSourceConfig {

	public static String getDriverClass(String dataBase) {
		
		if(dataBase.equalsIgnoreCase("MYSQL"))
			return new String("com.mysql.jdbc.Driver");
		if(dataBase.equalsIgnoreCase("MSSQL"))
			return new String("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		return null;
	}

}
