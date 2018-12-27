package com.power2sme.reporting.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.reporting.dao.ReportingDao;
import com.power2sme.reporting.entity.ReportingUser;
import com.power2sme.reporting.entity.TableInfo;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class WriteTableToExcel {

	@Autowired
	ReportingDao reportingDao;
	
	public void writeExcelSheet(ReportingUser user, TableInfo table, XSSFWorkbook workbook) throws SQLException {

		XSSFSheet sheet = workbook.createSheet(table.getSheetName());
		int rowNum = 0;
		List<List<String>> resultSet = new ArrayList<>();
		final int columnCount = reportingDao.getTableData(table, user, resultSet);
		boolean isHeader = true;
		for(List<String> rowResult :resultSet)
		{
			XSSFRow row = sheet.createRow(rowNum++);
			
			populateRow(row, rowResult, columnCount);
			if(isHeader)
			{
				isHeader= false;
			}
			if(rowNum == 100)
				log.info("Written "+rowNum+" rows of sheet:"+table.getSheetName());
		}
		
		autoSizeColumns(sheet, columnCount);
		
		
	}
	
	private void populateRow(XSSFRow row, List<String> rowResult, int columnCount) throws SQLException
	{
		for(int i =0;i< columnCount; i++)
		{
			row.createCell(i).setCellValue(rowResult.get(i));
		}
	}
	
	private void autoSizeColumns(XSSFSheet sheet, int columnCount)
	{
		log.info("Resizing columns");
		for(int i =0; i< columnCount; i++)
		{
			sheet.autoSizeColumn(i);
		}
	}


	
}
