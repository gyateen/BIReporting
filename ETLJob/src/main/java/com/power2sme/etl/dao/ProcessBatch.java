package com.power2sme.etl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.power2sme.etl.utils.BatchCounter;
import com.power2sme.etl.utils.ExecuteBatch;
import com.power2sme.etl.entity.ETLColumn;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.service.ETLStageRecord;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProcessBatch implements Runnable {

	
	List<ETLRecord> etlRecords ;
	PreparedStatement ps;
	BatchCounter executeBatchCount;
	Connection conn;
	String query;
	boolean isError = false;
	public static ExecutorService executor = Executors.newFixedThreadPool(10);
	
	public ProcessBatch(List<ETLRecord> etlRecords , String query,  Connection conn, BatchCounter executeBatchCount, boolean isError)
	{
		this.etlRecords = etlRecords;
		this.query = query;
		this.executeBatchCount = executeBatchCount;
		this.conn = conn;
		this.isError = isError;
	}
	
	@Override
	public void run() {
		try
		{
			
		PreparedStatement ps = conn.prepareStatement(query);	
		int batchRecordsCount = 0;
		for(ETLRecord row: etlRecords)
		{
			try
			{	
				int lastIndex=0;
				for(ETLColumn column: row.getColumns())
				{
					
					ps.setObject(column.getPosition(), column.getVal());
					lastIndex++;
					
				}
			if(isError)	
			{
				ETLStageRecord stageRecord= (ETLStageRecord) row;
				ps.setObject(++lastIndex, stageRecord.getRuleIds());
				ps.setObject(++lastIndex, stageRecord.getError().name());
				
			}
			ps.addBatch();
			batchRecordsCount++;
			}
			catch(SQLException ex)
			{
				log.info("Exception while inserting"+ex);
			}
			
		
			
		}
		
		executor.execute(new ExecuteBatch(batchRecordsCount, ps, conn, executeBatchCount));
		
		} catch (SQLException e) {
			log.info("Error while processing batch");
			e.printStackTrace();
			synchronized(conn)
			{
				
				executeBatchCount.incrementCounter();
				conn.notifyAll();
			}
		}

	}

}
