package com.power2sme.dms.client;

import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.power2sme.reporting.constants.ReportingConstants;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DMSClient {

	RestTemplate restTemplate = new RestTemplate();
	
	public FileInfo uploadDocument(String fileName) throws Exception
	{
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.MULTIPART_FORM_DATA);
		MultiValueMap<String, Object> requestBody
		  = new LinkedMultiValueMap<>();
		
		requestBody.add("file", new FileSystemResource(fileName));
		HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);
		
		log.info("Uploading document to dms: " + fileName);
		ResponseEntity<DMSUploadResponse> responseEntity = restTemplate.exchange(ReportingConstants.DMS_URL, HttpMethod.POST, requestEntity, DMSUploadResponse.class);
		DMSUploadResponse response= responseEntity.getBody();
		if(!response.getStatus().equalsIgnoreCase("success"))
			throw new Exception("File could not be uploaded to dms server");
		return response.getData();
		
	}
	
	public String getDownloadLink(String fileId)
	{
		StringBuilder link = new StringBuilder(ReportingConstants.DMS_DOWNLOAD_URL);
		link.append("?fileId=");
		link.append(fileId);
		return link.toString();
	}
}
