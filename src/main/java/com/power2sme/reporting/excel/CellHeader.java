package com.power2sme.reporting.excel;

import com.power2sme.reporting.excel.types.CellType;

import lombok.Data;

@Data
public class CellHeader {

	private CellType cellType;
	private String name;
	
}
