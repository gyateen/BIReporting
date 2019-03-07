package com.power2sme.reporting.mapper;

import java.util.HashMap;
import java.util.Map;


import com.power2sme.reporting.excel.types.CellType;

import java.sql.Types;


public class SQLExcelTypeMap {

	private Map<Integer, CellType> typeMap = new HashMap<>();
	
	
	public SQLExcelTypeMap()
	{
		init();
	}
	
	protected void init()
	{
		mapNumericTypes();
		mapDateTypes();
		
		
	}
	
	protected void mapDateTypes() {
		typeMap.put(Types.DATE, CellType.DATE);
		typeMap.put(Types.TIMESTAMP, CellType.DATE);
		typeMap.put(Types.TIME, CellType.DATE);
		
	}

	protected void mapNumericTypes() {
		typeMap.put(Types.BIGINT, CellType.NUMERIC);
		typeMap.put(Types.INTEGER, CellType.NUMERIC);
		typeMap.put(Types.FLOAT, CellType.NUMERIC);
		typeMap.put(Types.DECIMAL, CellType.NUMERIC);
		typeMap.put(Types.DOUBLE, CellType.NUMERIC);
		typeMap.put(Types.NUMERIC, CellType.NUMERIC);
		typeMap.put(Types.TINYINT, CellType.NUMERIC);
		typeMap.put(Types.SMALLINT, CellType.NUMERIC);
		typeMap.put(Types.REAL, CellType.NUMERIC);
		
	}

	public CellType getType(Integer key)
	{
		return typeMap.get(key);
	}
	
}
