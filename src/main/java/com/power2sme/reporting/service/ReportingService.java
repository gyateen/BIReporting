package com.power2sme.reporting.service;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.dms.client.DMSClient;
import com.power2sme.dms.client.FileInfo;
import com.power2sme.reporting.constants.ReportingConstants;
import com.power2sme.reporting.dao.ReportingDao;
import com.power2sme.reporting.entity.ReportingUser;
import com.power2sme.reporting.mail.MailService;
import com.power2sme.reporting.templates.ReportTemplate;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ReportingService {

	@Autowired
	ReportingDao reportingDao;
	
	@Autowired
	GenerateUserExcel excelService;
	
	@Autowired
	DMSClient dmsClient;
	
	@Autowired
	MailService mailService;
	
	public void generateAndSendReports()
	{
		ReportTemplate reportTemplate = reportingDao.getReport(ReportingConstants.REPORT_ID);
		generateAndSendReports(reportTemplate);
	}
	
	public void generateAndSendReports(ReportTemplate reportTemplate)
	{
		List<ReportingUser> reportingUser = reportingDao.getReportingUsers(reportTemplate.getReportId());
		for(ReportingUser user: reportingUser)
		{
			synchronized(user)
			{
				try
				{
					log.info("Creating report for user"+user.getUserEmail());
					String fileName = createReport(user, reportTemplate);
					log.info("Report creation complete for user"+user.getUserEmail());
					String link = uploadReportDMS(fileName);
					log.info("Report uploaded to dms successfully: "+ link);
					mailService.sendMail(mailService.getPayload(user.getUserEmail(), link, reportTemplate));
					log.info("Report sent to user:",user.getUserEmail());
				}
				catch(Exception ex)
				{
					log.error("Error:", ex);
				}
			}
		}
		
		
	}
	
	public String createReport(ReportingUser user, ReportTemplate reportTemplate) throws Exception
	{
		String fileName = generateUserFileName(user, reportTemplate);
		excelService.generateExcel(user, reportTemplate, fileName);
		return fileName;
	}
	
	public String uploadReportDMS(String report) throws Exception
	{
		FileInfo fileInfo = dmsClient.uploadDocument(report);
		if(fileInfo == null)
		{
			log.info("Upload report failed");
			return null;
		}
		String link = dmsClient.getDownloadLink(fileInfo.getFileId());
		return link;
		
	}
	
	private String generateUserFileName(ReportingUser user, ReportTemplate reportTemplate)
	{
		SimpleDateFormat formatter = new SimpleDateFormat(ReportingConstants.REPORTING_DATE_FORMAT);
		StringBuilder userDir = new StringBuilder(ReportingConstants.REPORTING_DIR);
		String userName = getUserName(user);
		userDir.append(File.separator);
		userDir.append(userName);
		File dir = new File(userDir.toString());
		if(!dir.exists())
			dir.mkdir();
		StringBuilder userFile = new StringBuilder(userDir);
		userFile.append(File.separator);
		userFile.append(reportTemplate.getReportName());
		userFile.append('_');
		userFile.append(userName);
		userFile.append('_');
		userFile.append(formatter.format(new Date()));
		userFile.append(".xlsx");
		return userFile.toString();
		
	}
	
	private String getUserName(ReportingUser user)
	{
		return user.getUserEmail().substring(0, user.getUserEmail().indexOf('@')).replace('.', '_');
	}
}
