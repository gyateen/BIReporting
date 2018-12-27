package com.power2sme.reporting.mail;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.reporting.constants.ReportingConstants;
import com.power2sme.reporting.templates.ReportTemplate;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MailService {

	MailSender mailSender;
	
	@Autowired
	MailService(MailSender mailSender)
	{
		this.mailSender = mailSender;
	}
	public void sendMail(String payload) {
		
		mailSender.callPostMQService(payload);
		
	}
	
	/*public String getPayload(String email, String link, String event)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat(ReportingConstants.REPORTING_DATE_FORMAT);
		StringBuilder payloadSb = new StringBuilder();
		payloadSb.append("<payload>");
		payloadSb.append("<object>notification</object>");
		payloadSb.append("<event>inside sales</event>");
		payloadSb.append("<cs_recipients>");
		StringBuilder mailTo = new StringBuilder("<mail_to><email_id>").append(email).append("</email_id></mail_to>");
		payloadSb.append(mailTo);
		payloadSb.append("</cs_recipients>");
		payloadSb.append("<cur_date>").append(dateFormat.format(new Date())).append("</cur_date>");
		payloadSb.append("<link>").append(link).append("</link>");
		payloadSb.append("</payload>");
		
		log.info(payloadSb.toString());
		
		return payloadSb.toString();
		
	}*/
	
	public String getPayload(String email, String link, ReportTemplate template)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat(ReportingConstants.REPORTING_DATE_FORMAT);
		String payloadSb = template.getTemplate();
		payloadSb = payloadSb.replaceAll("#EMAIL", email);
		payloadSb = payloadSb.replaceAll("#DATE", dateFormat.format(new Date()));
		payloadSb = payloadSb.replaceAll("#REPORT_LINK", link);
		payloadSb = payloadSb.replaceAll("#REPORT_NAME", template.getReportName());
		
		
		log.info(payloadSb);
		
		return payloadSb;
		
	}
	public String getTemplate()
	{
		String template = "<payload> 	<object>notification</object> 	<event>unapplied entries</event> 	<cs_recipients> 		<mail_to> 			<email_id>#EMAIL</email_id> 		</mail_to>	</cs_recipients> 	<cur_date>#DATE</cur_date> <link>#REPORT_LINK</link> <rpt_name>#REPORT_NAME</rpt_name> </payload>";
		return template;
	}
	

}
