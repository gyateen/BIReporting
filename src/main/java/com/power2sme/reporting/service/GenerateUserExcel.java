package com.power2sme.reporting.service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.reporting.dao.ReportingDao;
import com.power2sme.reporting.entity.ReportingUser;
import com.power2sme.reporting.entity.TableInfo;
import com.power2sme.reporting.templates.ReportTemplate;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GenerateUserExcel {

	@Autowired
	ReportingDao reportingDao;
	
	@Autowired
	WriteTableToExcel writeTable;
	
	
	public boolean generateExcel(ReportingUser user, ReportTemplate template, String fileName) throws IOException, SQLException
	{
		log.info("Generating excel for user:"+user.getUserEmail());
		XSSFWorkbook workbook = new XSSFWorkbook();
		List<TableInfo> userTables = reportingDao.getReportingTables(template.getReportId());
		if(userTables != null)
		{	
			for(TableInfo table: userTables)
			{
				writeTable.writeExcelSheet(user, table, workbook);
			}
		}
		writeExcel(fileName, workbook);
		
		workbook.close();
		return true;
	}
	
	private void writeExcel(String file, XSSFWorkbook workbook ) throws IOException
	{
		log.info("Writing excel at location: "+ file);
		FileOutputStream outputStream = new FileOutputStream(file);
		workbook.write(outputStream);
		outputStream.close();
		log.info("Excel written");
	}
	
	
	
}
