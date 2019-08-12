package com.power2sme.etl.request;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
@Scope(scopeName = "etl", proxyMode= ScopedProxyMode.TARGET_CLASS)
public class ETLRequest {

	
	private String database;
	private String table;
	private String schema;
	private String query;
	private String jobType;
	private int selectCount =0;

	public ETLRequest()
	{
		
	}
	
	public ETLRequest(String database, String table, String schema, String query, String jobType)
	{
		this.database = database;
		this.table = table;
		this.schema = schema;
		this.query = query;
		this.jobType = jobType;
	}
	
	public ETLRequest(String database, String table, String schema,  String jobType)
	{
		this.database = database;
		this.table = table;
		this.schema = schema;
		this.jobType = jobType;
	}
}
