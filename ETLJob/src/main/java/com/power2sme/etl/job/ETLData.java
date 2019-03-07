package com.power2sme.etl.job;

import java.util.List;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;

import lombok.Data;

@Data
public class ETLData {

	ETLRowHeader rowHeader;
	List<ETLRecord> etlRecords;
	
}
