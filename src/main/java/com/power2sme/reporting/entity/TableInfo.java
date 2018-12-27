package com.power2sme.reporting.entity;

import lombok.Data;

@Data
public class TableInfo {
	
	String tableName;
	String sheetName;
	String customQuery;
	
	public TableInfo()
	{
		
	}
	
	public TableInfo(String tableName, String sheetName)
	{
		this.tableName = tableName;
		this.sheetName = sheetName;
	}
	
		
}
