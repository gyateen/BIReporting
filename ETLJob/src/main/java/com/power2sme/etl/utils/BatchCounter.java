package com.power2sme.etl.utils;

import lombok.Data;

@Data
public class BatchCounter {

	int counter ;
	public BatchCounter(int i) {
		this.counter = i;
	}
	
	public void incrementCounter()
	{
		counter++;
	}

}
