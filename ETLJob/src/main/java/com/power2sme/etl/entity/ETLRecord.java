package com.power2sme.etl.entity;

import java.util.List;

import lombok.Data;

@Data
public class ETLRecord {

	List<ETLColumn> columns;
}
