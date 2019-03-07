package com.power2sme.etl.rules;

import lombok.Data;

@Data
public class ETLRule {

	String ruleId;
	String rule;
	RuleType ruleType;

	public ETLRule()
	{
		
	}
	
	public ETLRule(String ruleId, String rule)
	{
		this.ruleId = ruleId;
		this.rule = rule;
	}
	
	public ETLRule(String ruleId, String rule, RuleType ruleType)
	{
		this.ruleId = ruleId;
		this.rule = rule;
		this.ruleType = ruleType;
	}

}
