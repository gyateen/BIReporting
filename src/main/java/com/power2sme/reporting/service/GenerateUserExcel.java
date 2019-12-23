package com.power2sme.reporting.service;

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
	
	
	public XSSFWorkbook generateExcel(ReportingUser user, ReportTemplate template) throws IOException, SQLException
	{
		log.info("Generating excel for user:"+user.getUserEmail());
		XSSFWorkbook workbook = new XSSFWorkbook();
		List<TableInfo> userTables = reportingDao.getReportingTables(template.getReportId());
		int sheetWritten = 0;
		if(userTables != null)
		{	
			for(TableInfo table: userTables)
			{
				sheetWritten += writeTable.writeExcelSheet(user, table, workbook)?1:0;
			}
		}
		
	
		return sheetWritten ==0 ? null : workbook;
	}


	public XSSFWorkbook updateExcel(ReportingUser user, ReportTemplate reportTemplate, XSSFWorkbook workbook) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
