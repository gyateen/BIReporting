package com.power2sme.etl.service;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InsertBatchTask implements Runnable{

	String insertQuery;
	String targetDB;
	List<ETLRecord> etlRecords;
	ETLDao etlDao;
	ETLRowHeader header;
	public InsertBatchTask(ETLDao etlDao, String insertQuery, String targetDB,List<ETLRecord> etlRecords, ETLRowHeader header)
	{
		this.insertQuery = insertQuery;
		this.targetDB = targetDB;
		this.etlRecords = etlRecords;
		this.header = header;
		this.etlDao = etlDao;
	}
	
	@Override
	public void run() {
		try {
			etlDao.insertETLBatch(insertQuery, targetDB, etlRecords, header);
			log.info(new Date()+"batch task complete:"+etlRecords.size());
		} catch (SQLException e) {
			log.error("Error while inserting batch:"+e);
			e.printStackTrace();
		}
		
	}

	
}
