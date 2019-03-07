package com.power2sme.etl.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;

import com.power2sme.etl.constants.ETLConstants;
import com.power2sme.etl.entity.ETLColumn;
import com.power2sme.etl.entity.ETLColumnHeader;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;
import com.power2sme.etl.rules.ETLRule;
import com.power2sme.etl.staging.STAGE_ERROR;
import com.power2sme.etl.utils.ETLUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETLStaging {
	
	private static JexlEngine jexlEngine = new JexlEngine();
	private ETLRecord etlRecord;
	private ETLRowHeader header;
	private  Map<String, ETLRule> ruleMap;
	private Map<String, String[]> domainMap;
	private Map<String, Expression> qcMap;
	
	JexlContext jc; 
	
	public ETLStaging(JexlContext jc, Map<String, ETLRule> ruleMap, Map<String, String[]> domainMap, ETLRowHeader rowHeader)
	{
		this.header = rowHeader;
		this.jc = jc;
		this.ruleMap = ruleMap;
		this.domainMap = domainMap;
		prepareJexlContext();
		
	}
	
	public synchronized ETLRecord stage(ETLRecord etlRecord) {
		
		byte  error = 3;
		StringBuilder ruleIds = new StringBuilder();
		copy(etlRecord);
		Iterator<Entry<String, Expression>> qcIterator = qcMap.entrySet().iterator();
		while(qcIterator.hasNext())
		{
			
			Entry<String, Expression> entry  = qcIterator.next();
			String ruleId = entry.getKey();
			Expression expression = entry.getValue();
			try
			{	
				Boolean result = (Boolean) expression.evaluate(jc);
				if(result)
				{
					error = (byte) (error & ruleMap.get(ruleId).getRuleType().getVal());
					ruleIds.append(ruleId).append(ETLConstants.RULEID_DELIMITER);
				}
				
			}
			catch(JexlException ex)
			{
				log.error("Error while evaluating expression: "+ expression.getExpression()+ " "+ex);
			}
		}
		
		if(error  ==0)
			return new ETLStageRecord(etlRecord, STAGE_ERROR.ERROR, ruleIds.toString());
		if(error  ==1)
			return new ETLStageRecord(etlRecord, STAGE_ERROR.WARNING, ruleIds.toString());
		return new ETLStageRecord(etlRecord, STAGE_ERROR.PASS, ruleIds.toString());
		
		
	}

	private void copy(ETLRecord etlRecord)
	{
		int index = 0;
		for(ETLColumn column: this.etlRecord.getColumns())
		{
			column.setVal(etlRecord.getColumns().get(index).getVal());
			index++;
		}
	}
	
	
	private void prepareJexlContext()
	{
		etlRecord = new ETLRecord();
		etlRecord.setColumns(new ArrayList<ETLColumn>());
		log.info("Preparing jexl context for rules :"+ ruleMap.size());
		for(ETLColumnHeader colHeader: header.getColumnHeaders())
		{
			ETLColumn column = new ETLColumn();
			etlRecord.getColumns().add(column);
			String columnField = "row.".concat(colHeader.getName());
			jc.set(columnField, column);
			updateRuleMap(ruleMap, columnField, columnField.concat(".getVal()"));
		}
		
		setDomainContext(jc, domainMap);
		this.qcMap = createQCMap(ruleMap);
	}
	
	private void setDomainContext(JexlContext jc, Map<String, String[]> domainMap)
	{
		Iterator<Entry<String, String[]>> it = domainMap.entrySet().iterator();
		while(it.hasNext())
		{
			Entry<String, String[]> entry = it.next();
			jc.set(entry.getKey(), entry.getValue());
		}
	}

	private void updateRuleMap(Map<String, ETLRule> ruleMap, String originalString, String replaceString) {
		
		
		Iterator<Entry<String, ETLRule>> it = ruleMap.entrySet().iterator();
		while(it.hasNext())
		{
			Entry<String, ETLRule> entry = it.next();
			ETLRule rule = entry.getValue();
			rule.setRule(rule.getRule().replace(originalString, replaceString));
				
		}
		
	}

	private Map<String, Expression> createQCMap(Map<String, ETLRule> ruleMap )
	{
		return ETLUtils.map(ruleMap, jexlEngine);
	}
}
