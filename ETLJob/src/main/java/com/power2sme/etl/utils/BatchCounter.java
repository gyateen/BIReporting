package com.power2sme.etl.utils;

import lombok.Data;

@Data
public class BatchCounter {

	int counter ;
	int totalRecords = 0;
	public BatchCounter(int i) {
		this.counter = i;
	}
	
	public synchronized void updateTotalRecords(int records)
	{
		totalRecords += records;
	}
	
	public void incrementCounter()
	{
		counter++;
	}

}
