package com.power2sme.etl.entity;

import lombok.Data;

@Data
public class ETLQuery {
	
	String srcQuery;
	String targetQuery;
	String srcTable;
	String targetTable;
	String srcDB;
	String targetDB;

}
