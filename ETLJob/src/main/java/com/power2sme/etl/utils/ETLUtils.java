package com.power2sme.etl.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;

import com.power2sme.etl.entity.ETLColumn;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.mappers.CustomDataTypeMapper;
import com.power2sme.etl.rules.ETLRule;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETLUtils {

	private static CustomDataTypeMapper dataTypeMapper = new CustomDataTypeMapper();
	
	public static ETLRecord mapResultSetToETLRecord(ResultSet rs) throws SQLException {
		ETLRecord record = new ETLRecord();
		List<ETLColumn> columns = new ArrayList<>();
		for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
		{
			ETLColumn column = new ETLColumn();
			try
			{
				
				column.setPosition(i);	
				column.setVal(rs.getObject(i));
				dataTypeMapper.convertColumn(column, rs);
				
			}
			catch(RuntimeException ex)
			{
				log.error("Error while mapping column: "+ex);
			}
			columns.add(column);
		}	
		record.setColumns(columns);
		return record;
	}

	public static Map<String, Expression> map(Map<String, ETLRule> ruleMap, JexlEngine jexlEngine) {
		
		if(ruleMap == null)
			return null;
		Map<String, Expression> exprMap = new HashMap<>();
		Iterator<Entry<String, ETLRule>> it = ruleMap.entrySet().iterator();
		while(it.hasNext())
		{
			Entry<String, ETLRule> entry = it.next();
			String key = entry.getKey();
			Expression expr = jexlEngine.createExpression(entry.getValue().getRule());
			exprMap.put(key, expr);
		}
		return exprMap;
		
	}
	
	

}
