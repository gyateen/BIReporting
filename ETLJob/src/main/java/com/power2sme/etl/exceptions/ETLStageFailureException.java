package com.power2sme.etl.exceptions;

public class ETLStageFailureException extends RuntimeException {

private static final long serialVersionUID = 6592066888356877006L;
	
	public ETLStageFailureException(String msg)
	{
		super(msg);
	}
	
	public ETLStageFailureException(Exception ex)
	{
		super(ex);
	}
}
