package com.power2sme.etl.logging;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.dao.ETLLog;
import com.power2sme.etl.mail.MailService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LoggingService {

	@Autowired
	ETLDao etlDao;
	
	@Autowired
	MailService mailService;

	
	
	public ETLLog logJob(long runId, int jobId, Date startDate, Date endDate, String queryType, int recordsProcessed) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setRunId(runId);
		log.setJobId(jobId);
		log.setQueryType(queryType);
		log.setStatus("success");
		log.setRecordsProcessed(recordsProcessed);
		etlDao.insertLog(log);
		return log;
	}
	
	public ETLLog logJob(long runId, int jobId, Date startDate, Date endDate, String tableName, int recordsProcessed, Exception ex) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setJobId(jobId);
		log.setRunId(runId);
		log.setTable(tableName);
		log.setStatus("error");
		log.setRecordsProcessed(recordsProcessed);
		log.setError(ex.getMessage());
		etlDao.insertLog(log);
		
		return log;
		
	}
	
	
	public void logFailureAndSendMail(long runId, int jobId, Date startDate, Date endDate, String tableName, String jobType, int recordsProcessed, Exception ex) {
		
		try
		{
			ETLLog log = logJob(runId, jobId, startDate, endDate, tableName, recordsProcessed, ex);
			log.setQueryType(jobType);
			mailService.sendMail(mailService.getPayload(log));
		}
		catch(RuntimeException e)
		{
			log.error("Error while sending failure notification", e);
		}
	}
	
	
}
