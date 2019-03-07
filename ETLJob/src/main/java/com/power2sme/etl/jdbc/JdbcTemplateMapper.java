package com.power2sme.etl.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.power2sme.etl.config.DataSourceConfig;
import com.power2sme.etl.constants.ETLConstants;


@Component
public class JdbcTemplateMapper {

	private Map<String ,JdbcTemplate> mapper;
	
	public JdbcTemplateMapper()
	{
		mapper = new HashMap<>();
		addLoggingDataSource();
	}
	
	private void addLoggingDataSource()
	{
		mapper.put(ETLConstants.LOG_DATABASE, new JdbcTemplate(getLogDataSource()));
	}
	
	
	private DataSource getLogDataSource()
	{
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(DataSourceConfig.getDriverClass("MYSQL"));
		dataSource.setUrl(ETLConstants.LOG_DB_URL);
		dataSource.setUsername(ETLConstants.LOG_DB_USER);
		dataSource.setPassword(ETLConstants.LOG_DB_PASSWORD);
		return dataSource;

	}
	
	public void addDataSource(String dataBase, DataSource dataSource)
	{
		if(mapper.get(dataBase) != null)
			return;
		mapper.put(dataBase, new JdbcTemplate(dataSource));
	}
	
	
	private DataSource getNavDataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		dataSource.setUrl("jdbc:sqlserver://103.25.172.167:1433;");
		dataSource.setUsername("shweta");
		dataSource.setPassword("zsa!123");
		return dataSource;
	}	
	
	
	public JdbcTemplate getJdbcTemplate(String database) {
		return mapper.get(database);
	}
	
	

}
