package com.power2sme.etl.input;

import java.sql.SQLException;
import java.util.List;

import com.power2sme.etl.entity.ETLRowHeader;

public interface ETLReader<T> {

	ETLRowHeader getRowHeader();

	List<T> readNext(int i) throws SQLException;
	
	T readNext() throws SQLException;
	
	void closeReader();

	boolean hasRecord();
	

}
