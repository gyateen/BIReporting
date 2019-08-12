package com.power2sme.etl.logging;

import java.util.Date;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.dao.ETLLog;
import com.power2sme.etl.mail.MailService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LoggingService  implements DisposableBean, InitializingBean{

	@Autowired
	ETLDao etlDao;
	
	@Autowired
	MailService mailService;

	private Long runId;
	
	
	public Long getRunId()
	{
		return runId;
	}
	public ETLLog logJob(long runId, int jobId, Date startDate, Date endDate, String queryType, int recordsSelected, int recordsProcessed) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setRunId(runId);
		log.setJobId(jobId);
		log.setQueryType(queryType);
		log.setStatus("success");
		log.setRecordsProcessed(recordsProcessed);
		log.setRecordsSelected(recordsSelected);
		etlDao.insertLog(log);
		return log;
	}
	
	public ETLLog logJob(long runId, int jobId, Date startDate, Date endDate, String tableName, int recordsSelected,int recordsProcessed, Exception ex) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setJobId(jobId);
		log.setRunId(runId);
		log.setTable(tableName);
		log.setStatus("error");
		log.setRecordsProcessed(recordsProcessed);
		log.setRecordsSelected(recordsSelected);
		log.setError(ex.getMessage());
		etlDao.insertLog(log);
		
		return log;
		
	}
	
	
	public void logFailureAndSendMail(long runId, int jobId, Date startDate, Date endDate, String tableName, String jobType,int recordsSelected, int recordsProcessed, Exception ex) {
		
		try
		{
			ETLLog log = logJob(runId, jobId, startDate, endDate, tableName, recordsSelected, recordsProcessed, ex);
			log.setQueryType(jobType);
			mailService.sendMail(mailService.getPayload(log));
		}
		catch(RuntimeException e)
		{
			log.error("Error while sending failure notification", e);
		}
	}

	@Override
	public void destroy() throws Exception {
		log.info("Destroying logging service bean");
		etlDao.updateJobRunEndTime(runId, new Date());
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		
		runId = initiateJobRun();
	}
	
	protected Long initiateJobRun() {
		
		
		return etlDao.insertRunForJob();
	}
}
