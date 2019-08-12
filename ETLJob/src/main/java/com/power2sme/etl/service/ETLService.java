package com.power2sme.etl.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.JexlContext;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.job.ETLData;
import com.power2sme.etl.rules.ETLRule;

public interface ETLService {

	public ETLData select(String query);
	
	public int execute( ETLReader etlReader, String schema, String table, String stageTable);

	ETLReader<ETLRecord> selectForFuture(String query);

	public ETLReader<ETLRecord> stageRecordsInBatches(int jobId, ETLReader<ETLRecord> etlReader, JexlContext jexlContext,
			Map<String, ETLRule> ruleMap, Map<String, String[]> domainMap);

}
