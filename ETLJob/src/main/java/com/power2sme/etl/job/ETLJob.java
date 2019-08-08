package com.power2sme.etl.job;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.jexl2.JexlContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.power2sme.etl.config.DataSourceConfig;
import com.power2sme.etl.config.ETLConfig;
import com.power2sme.etl.entity.InputETLQuery;
import com.power2sme.etl.entity.OutputETLQuery;
import com.power2sme.etl.exceptions.ETLFailureException;
import com.power2sme.etl.exceptions.ETLStageFailureException;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.jdbc.JdbcTemplateMapper;
import com.power2sme.etl.logging.LoggingService;
import com.power2sme.etl.rules.ETLRule;
import com.power2sme.etl.service.ETLService;
import com.power2sme.etl.staging.ETLStager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class ETLJob {

	private static AnnotationConfigApplicationContext context;
	
	
	private static Long runId = null;
	
	
	public synchronized static void initContext()
	{
		context = (AnnotationConfigApplicationContext) SpringApplication.run(ETLJob.class);
	}
	
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
		initContext();
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
			String table = contextProp.getProperty("SRC_TABLE");
			log.info("Starting input job for job id: "+ jobId +" run id:"+runId);
			ETLReader etlReader = null;
			
			String schema = contextProp.getProperty("SRC_SCHEMA");
			String srcQuery = contextProp.getProperty("QRY");
			String jobType = contextProp.getProperty("JOB_TYPE");
			Date startDate = new Date();
			try
			{
				
	//			etlReader =  etlService.executeInputETLQueryForFuture(srcQuery,contextProp.getProperty("SRC_DATABASE") );
				etlReader =  etlService.selectForFuture(srcQuery);
	
			}
			
			catch(ETLFailureException ex)
			{
				log.error("Error while executing output job"+jobId+ " "+ex);
				loggingService.logFailureAndSendMail(runId, jobId, startDate, new Date(), schema.concat(".").concat(table),jobType, 0, ex);
				throw ex;
			}
			catch(RuntimeException ex)
			{
				loggingService.logJob(runId,jobId, startDate, new Date(), schema.concat(".").concat(table),0, ex);
				log.error("Error while executing job "+jobId+ " "+ex.getMessage());
				
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
		String jobType = contextProp.getProperty("JOB_TYPE");
		
		if(etlReader == null)
			return null;
		ETLReader stager = null;
		try
		{
			stager = etlService.stageRecordsInBatches(jobId, etlReader,jexlContext, ruleMap, domainMap);
		}
		catch(ETLFailureException ex)
		{
			log.error("Error while executing staging job"+jobId+ " "+ex);
			loggingService.logFailureAndSendMail(runId, jobId, jobStartDate, new Date(), schema.concat(".").concat(table),jobType,0, ex);
			throw ex;
		}
		catch(RuntimeException ex)
		{
			log.error("Error while executing staging job"+jobId+ " "+ex);
			loggingService.logJob(runId, jobId, jobStartDate, new Date(), schema.concat(".").concat(table),0, ex);
			
		}
		
		return stager;
			
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
		String jobType = contextProp.getProperty("JOB_TYPE");
	//	OutputETLQuery outputQuery = etlService.getETLQueryForOutput(table, stageTable, schema, etlReader.getRowHeader().getColumnHeaders().size(),null);
		int totalRecordsInserted = 0;
		try
		{
		//	totalRecordsInserted = etlService.executeOutputETLQuery(jobId, etlReader, outputQuery,contextProp.getProperty("TGT_DATABASE") );	
			totalRecordsInserted = etlService.execute(etlReader, schema, table, stageTable);
		}
		
		catch(ETLStageFailureException ex)
		{
			log.error("Error while executing staging job"+jobId+ " "+ex);
			loggingService.logFailureAndSendMail(runId, jobId, jobStartDate, new Date(), schema.concat(".").concat(stageTable), jobType,totalRecordsInserted, ex);
			throw ex;
		}
		catch(ETLFailureException ex)
		{
			log.error("Error while executing output job"+jobId+ " "+ex);
			loggingService.logFailureAndSendMail(runId, jobId, jobStartDate, new Date(), schema.concat(".").concat(table), jobType,totalRecordsInserted, ex);
			throw ex;
		}
		catch(RuntimeException ex)
		{
			log.error("Error while executing output job"+jobId+ " "+ex);
			loggingService.logJob(runId, jobId, jobStartDate, new Date(), jobType,totalRecordsInserted, ex);
			
		}
		finally
		{
			etlReader.closeReader();
		}
		
			
	}
	
	
		
}
	
