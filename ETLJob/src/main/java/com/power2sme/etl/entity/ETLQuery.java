package com.power2sme.etl.entity;

import lombok.Data;

@Data
public class ETLQuery {
	
	String query;
	String schema;
	String table;
	
}
