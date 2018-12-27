package com.power2sme.dms.client;


import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class DMSUploadResponse {

		
		@JsonProperty("message")
		private String message;
		@JsonProperty("errorCode")
		private int errorCode;
		@JsonProperty("totalRecords")  
		private int totalRecords;
		@JsonProperty("data") 
		private FileInfo data;
		@JsonProperty("status") 
		private String status;
		  

		 
		
		
}
