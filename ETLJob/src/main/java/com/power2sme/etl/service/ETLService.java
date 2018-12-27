package com.power2sme.etl.service;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.entity.ETLQuery;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ETLService {

	ETLDao etlDao;
	
	@Autowired
	ETLService(ETLDao etlDao)
	{
		this.etlDao = etlDao;
	}
	
	
	public void executeETLQuery(ETLQuery etlQuery)
	{
		try
		{
			log.info(new Date() + " Truncating table: " + etlQuery.getTargetTable());
			etlDao.truncateTable(etlQuery.getTargetTable(), etlQuery.getTargetDB());
			ETLRowHeader header = new ETLRowHeader();
			
			log.info(new Date()+" Executing query: "+ etlQuery.getSrcQuery());
			List<ETLRecord> records = etlDao.fetchETLRecords(etlQuery.getSrcQuery(), etlQuery.getSrcDB(), header);
			log.info(new Date()+ " total records count: "+records.size());
			String insertQuery = constructInsertQuery(etlQuery.getTargetTable(),etlQuery.getTargetDB(),header.getColumnHeaders().size());
			log.info("Executing insert query: "+insertQuery);
			Date jobStartDate = new Date();
			insertRecordsInBatches(insertQuery, etlQuery.getTargetDB(), records, header);
			Date jobEndDate = new Date();
			log.info("job Start time"+jobStartDate);
			
			log.info("job End time"+jobEndDate);
			log.info(new Date()+" Insert records complete");
		}
		catch(Exception ex)
		{
			log.error("Error while executing query:"+ex);
		}
	}
	
	public void insertRecordsInBatches(String insertQuery, String targetDB,List<ETLRecord> etlRecords, ETLRowHeader header) throws SQLException
	{
		int batchSize = 10000;
		List<List<ETLRecord>> batchList = Lists.partition(etlRecords, batchSize);
		int counter =1;
		
		
		for(List<ETLRecord> batch: batchList)
		{
			Date startDate = new Date();
			
			etlDao.insertETLBatch(insertQuery, targetDB, batch, header);
			Date endDate = new Date();
			log.info("Start time"+startDate);
			log.info("End time"+endDate);
			
			counter++;
		}
		
		
	}
	
	String constructInsertQuery(String table, String db, int columnCount)
	{
		StringBuilder query = new StringBuilder("insert into ");
		query.append(db).append(".").append(table);
		query.append(" values(");
		for(int i =0;i<columnCount-1;i++)
		{
			query.append('?').append(",");
		}
		
		query.append('?').append(')');
		return query.toString();
	}
	
	
		
	public void fetchAndExecuteJobs() {
		
		log.info("Fetching etlQueries from table");
		List<ETLQuery> etlQueries = etlDao.fetchqueriesForJob(1);
		for(ETLQuery etlQuery: etlQueries)
		{
			if(etlQuery.getSrcTable().equalsIgnoreCase("[BEBB_India$Cust_ Ledger Entry]"))
				executeETLQuery(etlQuery);
		}
	}
	
	public ETLQuery getETLQuery(String srcDB, String srcQuery, String targetDB, String targetTable, String targetQuery)
	{
		ETLQuery query = new ETLQuery();
		query.setSrcDB(srcDB);
		query.setSrcQuery(srcQuery);
		query.setTargetDB(targetDB);
		query.setTargetTable(targetTable);
		query.setTargetQuery(targetQuery);
		
		return query;
	}
	
	
}
