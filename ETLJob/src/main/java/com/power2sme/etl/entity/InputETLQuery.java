package com.power2sme.etl.entity;

import lombok.Data;

@Data
public class InputETLQuery extends ETLQuery {
	
	String lockMode = "READ";
}
