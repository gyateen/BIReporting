package com.power2sme.etl.mail;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.etl.constants.ETLConstants;
import com.power2sme.etl.dao.ETLLog;

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
	
	public String getPayload(ETLLog log)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat(ETLConstants.DATE_FORMAT);
		String payloadSb = getTemplate();
		payloadSb = payloadSb.replaceAll("#EMAIL", "yateen.gupta@power2sme.com");
		payloadSb = payloadSb.replaceAll("#START_TIME", dateFormat.format(log.getStartTime()));
		payloadSb = payloadSb.replaceAll("#END_TIME", dateFormat.format(log.getEndTime()));
		payloadSb = payloadSb.replaceAll("#TABLE", log.getTable());
		payloadSb = payloadSb.replaceAll("#JOB_TYPE", log.getQueryType());
		payloadSb = payloadSb.replaceAll("#JOB_ID", String.valueOf(log.getJobId()));
		payloadSb = payloadSb.replaceAll("#RUN_ID", String.valueOf(log.getRunId()));
		payloadSb = payloadSb.replaceAll("#FAILURE", log.getError());
		
		
		return payloadSb;
		
	}
	public String getTemplate()
	{
		String template = "\n" + 
				"		\n" + 
				"			<payload> 	<object>notification</object> 	<event>ods_etl_failure</event> 	<cs_recipients> 		<mail_to> <email_id>#EMAIL</email_id> 					</mail_to>	</cs_recipients> <error>\n" + 
				"	<start_time>#START_TIME</start_time><end_time>#END_TIME</end_time> <job_id>#JOB_ID</job_id><job_type>#JOB_TYPE</job_type> <run_id>#RUN_ID</run_id>\n" + 
				"<failure_reason>#FAILURE</failure_reason><table>#TABLE</table> </error>\n" + 
				" </payload>\n" + 
				"		\n" + 
				"\n" + 
				"	";
		return template;
	}
	

}
