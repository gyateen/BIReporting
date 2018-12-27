package com.power2sme.etl.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.power2sme.etl.constants.ETLConstants;

@Component
public class JdbcTemplateMapper {

	private Map<String ,JdbcTemplate> mapper;
	
	public JdbcTemplateMapper()
	{
		mapper = new HashMap<>();
		mapper.put("NAV", new JdbcTemplate(getNavDataSource()));
		mapper.put("P2S_BS", new JdbcTemplate(getODSDataSource()));
	}
	private DataSource getNavDataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		dataSource.setUrl("jdbc:sqlserver://103.25.172.167:1433;");
		dataSource.setUsername("shweta");
		dataSource.setPassword("zsa!123");
		return dataSource;
	}
	
	@Bean(name = "odsdatasource")
	public DataSource getODSDataSource() {

		
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(ETLConstants.DB_DRIVER);
		dataSource.setUrl(ETLConstants.DB_URL);
		dataSource.setUsername(ETLConstants.DB_USER);
		dataSource.setPassword(ETLConstants.DB_PASSWORD);
		return dataSource;
	}
	
	
	
	
	public JdbcTemplate getJdbcTemplate(String database) {
		return mapper.get(database);
	}
	
	

}
