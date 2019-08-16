package com.power2sme.reporting.service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.ss.formula.WorkbookEvaluator;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.power2sme.reporting.dao.ReportingDao;
import com.power2sme.reporting.entity.ReportingUser;
import com.power2sme.reporting.entity.TableInfo;
import com.power2sme.reporting.excel.CellHeader;
import com.power2sme.reporting.excel.types.CellType;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class WriteTableToExcel {

	
	@Autowired
	ReportingDao reportingDao;
	
	
	public boolean writeExcelSheet(ReportingUser user, TableInfo table, XSSFWorkbook workbook) throws SQLException {

		XSSFSheet sheet = workbook.createSheet(table.getSheetName());
		int rowNum = 0;
		List<List<Object>> resultSet = new ArrayList<>();
		List<CellHeader> headers = reportingDao.getTableDataForExcel(table, user, resultSet);
		if(headers == null || resultSet == null || resultSet.isEmpty())
			return false;
		XSSFRow row = sheet.createRow(rowNum++);
		populateHeader(row, headers);
		boolean isHeader = true;
		for(List<Object> rowResult :resultSet)
		{
			row = sheet.createRow(rowNum++);
			
			populateRow(row, rowResult, headers);
			if(isHeader)
			{
				isHeader= false;
			}
			if(rowNum == 100)
				log.info("Written "+rowNum+" rows of sheet:"+table.getSheetName());
		}
		
		autoSizeColumns(sheet, headers.size());
		return true;
		
	}
	

	public void writeToCSV(List<List<String>> data, String fileName) throws IOException
	{
		BufferedWriter writer = null;
		try
		{
			writer = new BufferedWriter(new FileWriter(fileName));
			if(data== null)
				return;
			for(List<String> row : data)
			{
				writeRow(row, writer);
				writer.flush();
			}
		}
		finally
		{
			if(writer != null)
				writer.close();
		}
				
	}
	
	private void writeRow(List<String> row, BufferedWriter writer) throws IOException {
		
		for(String column: row)
		{
			writer.write(column);
		}
		writer.newLine();
		
	}
	
	
	protected void populateHeader(XSSFRow row, List<CellHeader> headers) {
		for(int i=0;i< headers.size();i++)
		{
			XSSFCell cell = row.createCell(i);
			cell.setCellValue(headers.get(i).getName());
			cell.setCellStyle(getRowStyleHeader(row));
		}
		
	}
	
	protected void populateRow(XSSFRow row, List<Object> rowResult, List<CellHeader> headers) throws SQLException
	{
		XSSFWorkbook workbook = row.getSheet().getWorkbook();
		for(int i =0;i< headers.size(); i++)
		{
			CellType cellType = headers.get(i).getCellType();
			if(cellType == null)	
				cellType = CellType.STRING;
			XSSFCell cell = row.createCell(i);
			CellStyle style = workbook.createCellStyle();
			cell.setCellStyle(style);
			setCellBorder(style);
			try
			{
				switch(cellType)
				{
					
					case NUMERIC :
						cell.setCellValue(Double.valueOf(rowResult.get(i).toString()));
						break;
				
					case DATE:
						Object val = rowResult.get(i);
						
						if(val instanceof java.sql.Date)
						{
							cell.setCellValue(new Date(((java.sql.Date) val).getTime()) );
							cell.setCellStyle(getCellStyleDate(workbook, style));
						}
						else if(val instanceof java.sql.Timestamp)
						{
							cell.setCellValue(new Date(((java.sql.Timestamp) val).getTime()) );
							cell.setCellStyle(getCellStyleTimestamp(workbook, style));
						}
						
						
						else if(val instanceof java.sql.Time)
						{
							cell.setCellValue(new Date(((java.sql.Time) val).getTime()));
							cell.setCellStyle(getCellStyleTime(workbook, style));
						}
						
						else
							throw new IllegalArgumentException("value not of supported date types");
						break;
			
					default:
						cell.setCellValue(String.valueOf(rowResult.get(i)));
						break;
				}
			}
			catch(RuntimeException ex)
			{
				cell.setCellValue(String.valueOf(rowResult.get(i)));
			}
			XSSFFormulaEvaluator.evaluateAllFormulaCells(workbook);
			WorkbookEvaluator eval;
			
		}
	}
	
	private CellStyle getCellStyleTimestamp(XSSFWorkbook workbook, CellStyle style) {
		style.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd hh:mm:ss"));
		return style;
	}


	private CellStyle getCellStyleTime(XSSFWorkbook workbook,  CellStyle style) {
		
		style.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("hh:mm:ss"));
		return style;
	}


	private CellStyle getCellStyleDate(XSSFWorkbook workbook, CellStyle style) {
		style.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd"));
		return style;
	}

	protected CellStyle getRowStyleHeader(XSSFRow row)
	{
		XSSFWorkbook wb = row.getSheet().getWorkbook();
		CellStyle style = wb.createCellStyle();
		XSSFFont boldFont = wb.createFont();
		boldFont.setBold(true);
		boldFont.setFontHeight(10);
		
		style.setFont(boldFont);
		style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
		style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		setCellBorder(style);
		return style;
	}
	
	protected void setCellBorder(CellStyle style)
	{
		style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
		style.setBorderTop(HSSFCellStyle.BORDER_THIN);
		style.setBorderRight(HSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
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
