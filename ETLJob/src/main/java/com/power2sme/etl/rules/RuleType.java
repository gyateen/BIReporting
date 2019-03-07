package com.power2sme.etl.rules;

public enum RuleType {

	ERROR((byte)0), WARNING((byte)1);
	
	private byte val;
	RuleType(byte val)
	{
		this.val = val;
	}
	
	public byte getVal()
	{
		return val;
	}
}
