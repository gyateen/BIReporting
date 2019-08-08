package com.power2sme.etl.exceptions;

public class ETLFailureException extends RuntimeException {

	private static final long serialVersionUID = 6592066888356877006L;
	
	public ETLFailureException(String msg)
	{
		super(msg);
	}
	
	public ETLFailureException(Exception ex)
	{
		super(ex);
	}
}
