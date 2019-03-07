package com.power2sme.etl.logging;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.dao.ETLLog;

@Service
public class LoggingService {

	@Autowired
	ETLDao etlDao;

	public void logJob(long runId, int jobId, Date startDate, Date endDate, String queryType, int recordsProcessed) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setRunId(runId);
		log.setJobId(jobId);
		log.setQueryType(queryType);
		log.setStatus("success");
		log.setRecordsProcessed(recordsProcessed);
		etlDao.insertLog(log);
	}
	
public void logJob(long runId, int jobId, Date startDate, Date endDate, String queryType, int recordsProcessed, Exception ex) {
		
		ETLLog log = new ETLLog();
		log.setEndTime(endDate);
		log.setStartTime(startDate);
		log.setJobId(jobId);
		log.setRunId(runId);
		log.setQueryType(queryType);
		log.setStatus("error");
		log.setRecordsProcessed(recordsProcessed);
		log.setError(ex.getMessage());
		etlDao.insertLog(log);
	}
	
	
}
