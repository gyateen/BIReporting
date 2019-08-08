package com.power2sme.reporting.config;

import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.power2sme.reporting.constants.ReportingConstants;
import com.power2sme.reporting.mapper.SQLExcelTypeMap;

@Configuration
@ComponentScan(basePackages = {"com.power2sme.reporting", "com.power2sme.dms.client"})
public class ReportingConfig {
	
	
	@Bean
	DataSource getDataSource()
	{
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(ReportingConstants.DB_DRIVER);
		dataSource.setUrl(ReportingConstants.DB_URL);
		dataSource.setUsername(ReportingConstants.DB_USER);
		dataSource.setPassword(ReportingConstants.DB_PASSWORD);
		return dataSource;
	}
	
	@Bean
	SQLExcelTypeMap getExcelTypeMap()
	{
		return new SQLExcelTypeMap();
	}
	
	@Bean
	SessionFactory getSessionFactory()
	{
		return new org.hibernate.cfg.Configuration().buildSessionFactory();
	}
}
