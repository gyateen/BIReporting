package com.power2sme.etl.service;

import java.util.List;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.job.ETLData;

public interface ETLService {

	public ETLData select(String query);
	
	public int execute( ETLReader etlReader, String schema, String table, String stageTable);

	ETLReader<ETLRecord> selectForFuture(String query);
}
