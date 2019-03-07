package com.power2sme.etl.routines;

public class DomainCheckRoutine {

	 public static boolean DomainCheckRoutinee(Object message,String[] results) {
	    	  
	        String msg = String.valueOf(message);
	        for(int i=0;i<results.length;i++){
	        	if(results[i].equals(msg)){
	        		return true;
	        	}
	        }
	        return false;
	    }
}
