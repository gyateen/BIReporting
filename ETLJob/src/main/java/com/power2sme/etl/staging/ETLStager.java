package com.power2sme.etl.staging;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.service.ETLStageRecord;
import com.power2sme.etl.service.ETLStaging;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETLStager<T> implements ETLReader<ETLRecord> {

	ETLReader<ETLRecord> etlReader;
	ETLStaging etlStaging;
	
	public ETLStager(ETLReader<ETLRecord> etlReader, ETLStaging etlStaging) {
		this.etlReader = etlReader;
		this.etlStaging = etlStaging;
	}

	@Override
	public ETLRowHeader getRowHeader() {
		return etlReader.getRowHeader();
	}

	@Override
	public List<ETLRecord> readNext(int i) throws SQLException {
		List<ETLRecord> etlRecords = etlReader.readNext(i);
		log.info("Total select records read: "+etlRecords.size());
		List<ETLRecord> stageRecords = new ArrayList<>();
		
		for(ETLRecord etlRecord: etlRecords)
		{
			stageRecords.add(etlStaging.stage(etlRecord));
		}
		return stageRecords;
	}

	@Override
	public ETLRecord readNext() throws SQLException {
		
		ETLRecord etlRecord = etlReader.readNext();
		if(etlRecord != null)
			return etlStaging.stage(etlRecord);
		return etlRecord;
	}

	@Override
	public void closeReader() {
		etlReader.closeReader();

	}

	@Override
	public boolean hasRecord() {
		
		return etlReader.hasRecord();
	}

}
