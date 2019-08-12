package com.power2sme.etl.dao;

import java.util.Date;

import lombok.Data;

@Data
public class ETLLog {

	
	int jobId;
	long runId;
	Date startTime;
	Date endTime;
	String queryType;
	String table;
	int recordsProcessed = 0;
	int recordsSelected = 0;
	String status;
	String error;
	
	
	
	
}
