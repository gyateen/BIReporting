package com.power2sme.etl.job;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.jexl2.JexlContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.power2sme.etl.config.DataSourceConfig;
import com.power2sme.etl.config.ETLConfig;
import com.power2sme.etl.entity.InputETLQuery;
import com.power2sme.etl.entity.OutputETLQuery;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.jdbc.JdbcTemplateMapper;
import com.power2sme.etl.logging.LoggingService;
import com.power2sme.etl.rules.ETLRule;
import com.power2sme.etl.service.ETLService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETLJob {

	private static AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ETLConfig.class);
	
	private static LoggingService loggingService = context.getBean(LoggingService.class);
	
	private static Long runId = null;
	private static void configureDataSources(Properties contextProp)
	{
		JdbcTemplateMapper jdbcMapper = context.getBean(JdbcTemplateMapper.class);
		String srcDataBase = contextProp.getProperty("SRC_DATABASE");
		jdbcMapper.addDataSource(srcDataBase, getDataSourceForSource(contextProp));
		String targetDataBase = contextProp.getProperty("TGT_DATABASE");
		jdbcMapper.addDataSource(targetDataBase, getDataSourceForTarget(contextProp));
		
	}
	
	private static synchronized void generateRunId()
	{
		if(runId == null)
		{
			ETLService etlService = context.getBean(ETLService.class);
			runId = etlService.initiateJobRun();
		}
	}
	
	public static long initJob(Properties contextProp)
	{
		configureDataSources(contextProp);
		generateRunId();
		log.info("Component initialization complete");
		return runId;
	}
	
	
	
	private static DataSource getDataSourceForTarget(Properties contextProp) {
		
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(DataSourceConfig.getDriverClass(contextProp.getProperty("TGT_DB_SERVER")));
		dataSource.setUrl(contextProp.getProperty("TGT_URL"));
		dataSource.setUsername(contextProp.getProperty("TGT_USER"));
		dataSource.setPassword(contextProp.getProperty("TGT_PASSWORD"));
		return dataSource;
	}



	private static DataSource getDataSourceForSource(Properties contextProp) {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(DataSourceConfig.getDriverClass(contextProp.getProperty("SRC_DB_SERVER")));
		dataSource.setUrl(contextProp.getProperty("SRC_URL"));
		dataSource.setUsername(contextProp.getProperty("SRC_USER"));
		dataSource.setPassword(contextProp.getProperty("SRC_PASSWORD"));
		return dataSource;
	}


	public static ETLReader runInputJob(Long runId, Properties contextProp)
	{
		
		
			ETLService etlService = context.getBean(ETLService.class);
			int jobId = Integer.parseInt(contextProp.getProperty("JOB_ID"));
			log.info("Starting input job for job id: "+ jobId +" run id:"+runId);
			ETLReader etlReader = null;
			
			String schema = contextProp.getProperty("SRC_SCHEMA");
			String srcQuery = contextProp.getProperty("QRY");
			InputETLQuery inputQuery = etlService.getETLQueryForInput(null, schema, srcQuery);	
			Date startDate = new Date();
			try
			{
				
				etlReader =  etlService.executeInputETLQueryForFuture(inputQuery,contextProp.getProperty("SRC_DATABASE") );
	
			}
			catch(RuntimeException ex)
			{
				loggingService.logJob(runId,jobId, startDate, new Date(), "select",0, ex);
				log.error("Error while executing job "+jobId+ " "+ex);
				
			}
			
			
			return etlReader;
	}
	
	public static ETLReader runStagingJob(Long runId, Properties contextProp, ETLReader etlReader, JexlContext jexlContext, Map<String, ETLRule> ruleMap, Map<String, String[]> domainMap)
	{
		int jobId = Integer.parseInt(contextProp.getProperty("JOB_ID"));
		ETLService etlService = context.getBean(ETLService.class);
		log.info("Starting staging job for job id: "+ jobId +" run id:"+runId);
		String schema = contextProp.getProperty("TGT_SCHEMA");
		String table = contextProp.getProperty("TGT_TABLE");
		Date jobStartDate = new Date();
	
		
		if(etlReader == null)
			return null;				
		return etlService.stageRecordsInBatches(jobId, etlReader,jexlContext, ruleMap, domainMap);
			
	}
	
	public static void runOutputJob(long runId, ETLReader etlReader, Properties contextProp)
	{
		int jobId = Integer.parseInt(contextProp.getProperty("JOB_ID"));
		ETLService etlService = context.getBean(ETLService.class);
		log.info("Starting output job for job id: "+ jobId +" run id:"+runId);
		String schema = contextProp.getProperty("TGT_SCHEMA");
		String table = contextProp.getProperty("TGT_TABLE");
		String stageTable = contextProp.getProperty("TGT_STAGE_TABLE");
		Date jobStartDate = new Date();
	
		if(etlReader == null)
			return;				
		OutputETLQuery outputQuery = etlService.getETLQueryForOutput(table, stageTable, schema, etlReader.getRowHeader().getColumnHeaders().size(),null);
		int totalRecordsInserted = etlService.executeOutputETLQuery(jobId, etlReader, outputQuery,contextProp.getProperty("TGT_DATABASE") );	
		etlReader.closeReader();
		loggingService.logJob(runId, jobId, jobStartDate, new Date(), "insert",totalRecordsInserted);
			
	}
	
	
		
}
	
