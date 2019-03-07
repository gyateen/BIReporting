package com.power2sme.etl.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.power2sme.etl.entity.ETLRowHeader;


public abstract class ETLRowMapper<T> implements RowMapper<T> {
	
	private boolean isHeaderPopulated = false;
	private ETLRowHeader header;
	
	
	public ETLRowMapper(ETLRowHeader header) {
		this.header = header;
	}

	public boolean isheaderPopulated()
	{
		return isHeaderPopulated;
	}
	
	public void headerPopulated()
	{
		isHeaderPopulated = true;
	}
	
	public void setRowHeader(ETLRowHeader header)
	{
		this.header = header;
	}
	
	public ETLRowHeader getRowHeader()
	{
		return header;
	}
	
	public abstract void populateColumnHeader(ResultSet rs) throws SQLException;
}
