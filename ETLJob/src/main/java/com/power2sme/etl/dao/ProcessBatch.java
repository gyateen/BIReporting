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

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProcessBatch implements Runnable {

	
	List<ETLRecord> etlRecords ;
	PreparedStatement ps;
	BatchCounter executeBatchCount;
	Connection conn;
	String query;
	public static ExecutorService executor = Executors.newFixedThreadPool(5);
	
	public ProcessBatch(List<ETLRecord> etlRecords , String query, Connection conn, BatchCounter executeBatchCount)
	{
		this.etlRecords = etlRecords;
		this.query = query;
		this.executeBatchCount = executeBatchCount;
		this.conn = conn;
	}
	
	@Override
	public void run() {
		try
		{
			
		PreparedStatement ps = conn.prepareStatement(query);	
		for(ETLRecord row: etlRecords)
		{
				
			for(ETLColumn column: row.getColumns())
			{
				try
				{
				ps.setObject(column.getPosition(), column.getVal());
				}
				catch(Exception ex)
				{
					log.info("Exception while inserting"+ex);
				}
			}
			
			ps.addBatch();
		
			
			
			
		}
		executor.execute(new ExecuteBatch(ps, conn, executeBatchCount));
		
		} catch (SQLException e) {
			log.info("Error while processing batch");
			e.printStackTrace();
		}
		
		finally
		{
			
		}

	}

}
