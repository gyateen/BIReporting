package com.power2sme.etl.job;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.power2sme.etl.request.ETLRequest;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Component
@Scope(scopeName = "etl", proxyMode= ScopedProxyMode.TARGET_CLASS)
@EqualsAndHashCode(callSuper=true)
public class ETLStageRequest extends ETLRequest {

	private String stageTable;
	
	public ETLStageRequest(String database, String table, String stageTable, String schema,  String jobType)
	{
		super(database, table, schema, jobType);
		this.stageTable = stageTable;
	}
}
