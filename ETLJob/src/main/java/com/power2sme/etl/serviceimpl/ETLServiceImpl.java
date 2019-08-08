package com.power2sme.etl.serviceimpl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.JexlContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;
import com.power2sme.etl.dao.ETLDao;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;
import com.power2sme.etl.entity.InputETLQuery;
import com.power2sme.etl.entity.OutputETLQuery;
import com.power2sme.etl.exceptions.ETLFailureException;
import com.power2sme.etl.exceptions.ETLStageFailureException;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.job.ETLData;
import com.power2sme.etl.logging.LoggingService;
import com.power2sme.etl.rules.ETLRule;
import com.power2sme.etl.service.ETLService;
import com.power2sme.etl.service.ETLStageRecord;
import com.power2sme.etl.service.ETLStaging;
import com.power2sme.etl.staging.ETLStager;
import com.power2sme.etl.staging.STAGE_ERROR;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ETLServiceImpl implements ETLService{

	ETLDao etlDao;
	LoggingService loggingService;
	
	@Autowired
	public ETLServiceImpl(ETLDao etlDao, LoggingService loggingService)
	{
		this.etlDao = etlDao;
		this.loggingService = loggingService;
	}
	
	@Override
	public ETLData select(String query) {
		log.info("Executing select query: "+query);
		ETLRowHeader header = new ETLRowHeader();
		List<ETLRecord> records = etlDao.fetchETLRecords(query,header);
		ETLData etlData = new ETLData();
		etlData.setRowHeader(header);
		etlData.setEtlRecords(records);
		return etlData;
	}
	
	@Override
	public ETLReader<ETLRecord> selectForFuture(String query)
	{
		log.info("Executing select query: "+query);
		ETLRowHeader header = new ETLRowHeader();
		
		ETLReader<ETLRecord> etlReader = etlDao.fetchETLReader(query , header);
		return etlReader;
	}
	
	@Override
	public int execute(ETLReader etlReader, String schema, String table, String stageTable)
	{
		String stageQuery = null;
		String query = constructInsertQuery(table,schema, etlReader.getRowHeader().getColumnHeaders().size());
		
		etlDao.truncateTable(table,schema);
		log.info("Executing insert query: "+ query);
		
		  if(etlReader instanceof ETLStager) {
			
		  stageQuery = constructInsertQuery(stageTable, schema,etlReader.getRowHeader().getColumnHeaders().size() + 2);
		  etlDao.truncateTable(stageTable,schema); 
		  }
		 
	return insertRecordsInBatches(query, stageQuery,etlReader);
			
	}
	public int insertRecordsInBatches(int jobId, String insertQuery, List<ETLRecord> etlRecords)
	{
		int batchSize = 10000;
		List<List<ETLRecord>> batchList = Lists.partition(etlRecords, batchSize);
		int transactionBatches = batchList.size();
		int recordsInserted = 0;
		int batchCounter = 0;
		try
		{
			for(List<ETLRecord> batch: batchList)
			{
				
				Date startDate = new Date();
				
				try
				{
					recordsInserted += etlDao.insertETLBatch(insertQuery, batch, false);
					log.info("Job ID " + jobId+" records inserted: "+ recordsInserted);
					
				}
				catch(SQLException ex)
				{
					log.error("Error while inserting batch "+ex);
				}
				Date endDate = new Date();
				log.info("Start time"+startDate);
				log.info("End time"+endDate);
				batchCounter++;
				if(batchCounter < transactionBatches)
					batch.clear();
			}
		}
		catch(RuntimeException ex)
		{
			log.error("Error while iterating through transaction batch list" + ex);
		}
		
		return recordsInserted;
		
		
	}
	
	public static String constructInsertQuery(String table, String schema, int columnCount)
	{
		StringBuilder query = new StringBuilder("insert into ");
		query.append(schema).append(".").append(table);
		query.append(" values(");
		for(int i =0;i<columnCount-1;i++)
		{
			query.append('?').append(",");
		}
		
		query.append('?').append(')');
		return query.toString();
	}
	
		


	public InputETLQuery getETLQueryForInput(String table, String srcSchema, String srcQuery) {
		
		InputETLQuery query = new InputETLQuery();
		query.setQuery(srcQuery);
		query.setSchema(srcSchema);
		query.setTable(table);
		return query;
	}

	public OutputETLQuery getETLQueryForOutput(String table, String stageTable, String schema, int columnCount, String targetQuery) {
		
		String query = targetQuery;
		if(query == null)
			query = constructInsertQuery(table,schema, columnCount);
		OutputETLQuery etlQuery = new OutputETLQuery();
		etlQuery.setQuery(query);
		etlQuery.setSchema(schema);
		etlQuery.setTable(table);
		etlQuery.setStageTable(stageTable);
		return etlQuery;
	}

	public long initiateJobRun() {
		
		
		return etlDao.insertRunForJob();
	}

	public int insertRecordsInBatches(String insertQuery, String stageQuery, ETLReader<ETLRecord> etlReader) {
		
		int recordsInserted = 0;
		List<ETLRecord> batch = null;
		while(etlReader.hasRecord())
		{
			List<ETLRecord> passBatch =null;
			List<ETLRecord> errorBatch =null;
			try
			{
				batch = etlReader.readNext(10000);
				log.info("Processing batch of size: "+ batch.size());
				if(etlReader instanceof ETLStager)
				{
					passBatch = new ArrayList<>();
					errorBatch = new ArrayList<>();
					for(ETLRecord record: batch)
					{
						ETLStageRecord stageRecord = (ETLStageRecord) record;
						if(stageRecord.getError() == STAGE_ERROR.ERROR || stageRecord.getError() == STAGE_ERROR.WARNING)
							errorBatch.add(record);
						
						if(stageRecord.getError() != STAGE_ERROR.ERROR)
							passBatch.add(record);
						
					}
					batch = null;
				}
				else
					passBatch = batch;
				
				if(passBatch != null)
					log.info("Passed records read batch size: "+ passBatch.size() );
				if(errorBatch != null)
					log.info("Failed records read batch size: "+ errorBatch.size() );
			}
			catch(SQLException ex)
			{
				log.error("Error while fetching records: "+ex);
			}
			Date startDate = new Date();
			try
			{
				if(passBatch !=null)
					recordsInserted += etlDao.insertETLBatch(insertQuery, passBatch, false);
				if(errorBatch != null)
					recordsInserted += etlDao.insertETLBatch(stageQuery, errorBatch, true);
				log.info("Total records inserted in this batch: "+ recordsInserted);
				
			}
			catch(SQLException ex)
			{
				log.error("Error while inserting batch "+ex);
			}
			Date endDate = new Date();
			log.info("Start time"+startDate);
			log.info("End time"+endDate);
		}	
		return recordsInserted;
	}
	
public ETLReader<ETLRecord> stageRecordsInBatches(int jobId, ETLReader<ETLRecord> etlReader, JexlContext jc,Map<String, ETLRule> ruleMap,Map<String, String[]> domainMap ) {
		
		ETLStaging etlStaging = new ETLStaging(jc,ruleMap, domainMap, etlReader.getRowHeader());
	 	
		ETLReader<ETLRecord> etlStager = new ETLStager<ETLRecord>(etlReader, etlStaging);
		return etlStager;
	}

	
	
}

