package com.power2sme.etl.mappers;


import java.sql.ResultSet;

import org.springframework.stereotype.Component;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.power2sme.etl.entity.ETLColumn;


@Component
public class CustomDataTypeMapper {

	
	public void convertColumn(ETLColumn column, ResultSet rs)
	{
		Object val = column.getVal();
		if(rs instanceof SQLServerResultSet && val.getClass() == byte[].class)		
		{
				column.setVal(convertByteArrayToHex((byte[]) val));
			
		}
		
	}
	
	private static String convertByteArrayToHex(byte[] bytes)
	{
		 StringBuilder sb = new StringBuilder();
		    for (byte b : bytes) {
		        sb.append(String.format("%02X", b));
		    }
		    return sb.toString();
	}
}
