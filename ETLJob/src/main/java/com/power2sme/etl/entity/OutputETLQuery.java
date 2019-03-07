package com.power2sme.etl.entity;

import lombok.Data;

@Data
public class OutputETLQuery extends ETLQuery {

	String truncateMode = "TRUNCATE";
	String stageTable;
}
