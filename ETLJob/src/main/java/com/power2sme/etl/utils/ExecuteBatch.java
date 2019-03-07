package com.power2sme.etl.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecuteBatch implements Runnable {

	PreparedStatement ps;
	Connection conn;
	int batchCount;
	BatchCounter executeBatchCount;
	
	public ExecuteBatch(int batchCount, PreparedStatement ps, Connection conn, BatchCounter executeBatchCount) {
		this.ps = ps;
		this.conn = conn;
		this.executeBatchCount = executeBatchCount;
		this.batchCount = batchCount;
	}

	@Override
	public void run() {
		try {
			
			ps.executeBatch();
			executeBatchCount.updateTotalRecords(batchCount);
		} catch (SQLException e) {
			log.info("Error while executing batch");
			e.printStackTrace();
		}
		finally
		{
				try {
					if(ps!=null)
					ps.close();
				} catch (SQLException e) {
					log.error("Error while closing preparedstatement: "+e);
				}
				synchronized(conn)
				{
					
					executeBatchCount.incrementCounter();
					conn.notifyAll();
				}
		}
			
	}

	private void updateBatchCount(BatchCounter executeBatchCount, int[] updates) {
		
		int updateCount = 0;	
		for(int update: updates)
			{
				updateCount += (update <0)?0:1;
			}
		log.info("update count: "+updateCount);
		executeBatchCount.updateTotalRecords(updateCount);
		
	}

}
