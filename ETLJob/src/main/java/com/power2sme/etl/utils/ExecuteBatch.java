package com.power2sme.etl.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecuteBatch implements Runnable {

	PreparedStatement ps;
	Connection conn;
	BatchCounter executeBatchCount;
	
	public ExecuteBatch(PreparedStatement ps, Connection conn, BatchCounter executeBatchCount) {
		this.ps = ps;
		this.conn = conn;
		this.executeBatchCount = executeBatchCount;
	}

	@Override
	public void run() {
		try {
			
			ps.executeBatch();
			ps.close();
		} catch (SQLException e) {
			log.info("Error while executing batch");
			e.printStackTrace();
		}
		finally
		{
			synchronized(conn)
			{
				
				executeBatchCount.incrementCounter();
		//		log.info("Total batches executed:"+ executeBatchCount.getCounter());
				conn.notifyAll();
			}
		}
	}

}
