package com.power2sme.etl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.power2sme.etl.entity.ETLColumnHeader;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;
import com.power2sme.etl.input.RunnableETLReader;
import com.power2sme.etl.utils.ETLUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FutureETLReader implements RunnableETLReader<ETLRecord> {

	Connection conn;
	PreparedStatement ps;
	ResultSet rs;
	ETLRowHeader header;
	boolean closeReader = false;
	Date startDate;
	Date endDate;
	
	public FutureETLReader(Connection conn, PreparedStatement ps, ResultSet rs) throws SQLException {
		this.conn = conn;
		this.ps = ps;
		this.rs = rs;
		this.header = constructRowHeader(rs);
		
	}

	private static ETLRowHeader constructRowHeader(ResultSet rs) throws SQLException
	{
		ETLRowHeader header = new ETLRowHeader();
		List<ETLColumnHeader> columnHeaders = new ArrayList<>(); 
		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		{
			ETLColumnHeader columnHeader = new ETLColumnHeader();
			columnHeader.setName(rs.getMetaData().getColumnName(i));
			columnHeader.setType(rs.getMetaData().getColumnType(i));
			columnHeader.setLength(rs.getMetaData().getPrecision(i));
			columnHeaders.add(columnHeader);
		}
		header.setColumnHeaders(columnHeaders);
		return header;

	}
	@Override
	public ETLRowHeader getRowHeader() {
		return this.header;
	}

	@Override
	public List<ETLRecord> readNext(int i) throws SQLException {
		int counter = 0;
		List<ETLRecord> etlRecords = new ArrayList<>();
		while(counter < i && rs.next() )
		{
			etlRecords.add(ETLUtils.mapResultSetToETLRecord(rs));
			counter++;
		}
		return etlRecords;
	}

	@Override
	public ETLRecord readNext() throws SQLException {
		
		if(rs.next())
		{
			return ETLUtils.mapResultSetToETLRecord(rs);
		}
		return null;
	}
	
	
	@Override
	public void run() {
		
		try
		{
			while(!closeReader)
			{
				synchronized(this)
				{
					if(!closeReader)
						try {
							wait();
						} catch (InterruptedException e) {
							log.info("Etl reader interrupted while waiting");
							
						}
				}
			}
			log.info("Closing the reader");
		}
		finally
		{
			try
			{
			if(rs != null)
				rs.close();
			}
			catch(SQLException ex)
			{
				log.error("Error while closing connection: "+ex);
			}
		}
	}

	@Override
	public synchronized void closeReader() {
			closeReader  = true;
			notifyAll();
		
	}

	@Override
	public boolean hasRecord(){
		
		try {
			return !rs.isAfterLast();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
