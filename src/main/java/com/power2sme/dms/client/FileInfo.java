package com.power2sme.dms.client;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class FileInfo {
	
	@JsonProperty("fileId")
	String fileId;
	
	@JsonProperty("fileName")
	String fileName;
	
	@JsonProperty("fileSize")
	String fileSize;

}
