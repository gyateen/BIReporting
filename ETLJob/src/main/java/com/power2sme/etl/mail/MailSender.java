package com.power2sme.etl.mail;


import javax.ws.rs.core.MediaType;

import org.springframework.stereotype.Component;

import com.power2sme.etl.constants.ETLConstants;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MailSender {
	
	public static final String MQ_URL= ETLConstants.MQ_URL;
	
	
	
	public String callPostMQService(String paylod) {
		
		Client client = Client.create();
		
		
		log.info("mqUrl = ", MQ_URL);
		WebResource webResource = client.resource(MQ_URL);
		paylod = paylod.replaceAll("&", "&amp\\;");
		paylod = paylod.replaceAll("'", "&#39\\;");
		Form formData = new Form();
		formData.add("method", "enqueue");
		formData.add("payload", paylod);
		log.debug("payload for MQ: " + paylod);
		ClientResponse response = webResource.type(MediaType.APPLICATION_FORM_URLENCODED).post(ClientResponse.class, formData);
 
		String output = response.getEntity(String.class);		
		log.info("MQ Response status = " + response.getStatus());
		log.info("MQ Response text = " + output);
		
		return output;
	}
}
