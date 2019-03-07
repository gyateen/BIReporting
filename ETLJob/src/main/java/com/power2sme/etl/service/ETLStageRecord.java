package com.power2sme.etl.service;

import java.util.List;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.staging.STAGE_ERROR;

import lombok.Data;

@Data
public class ETLStageRecord extends ETLRecord {

	private STAGE_ERROR error;
	private String ruleIds;
	public ETLStageRecord(ETLRecord etlRecord, STAGE_ERROR error, String ruleIds) {
		this.setColumns(etlRecord.getColumns());
		this.error = error;
		this.ruleIds = ruleIds;
	}

}
