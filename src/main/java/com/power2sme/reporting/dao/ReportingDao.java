package com.power2sme.reporting.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.power2sme.reporting.constants.QueryConstants;
import com.power2sme.reporting.entity.ReportingUser;
import com.power2sme.reporting.entity.TableInfo;
import com.power2sme.reporting.templates.ReportTemplate;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class ReportingDao {

	private DataSource dataSource;
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	public ReportingDao(DataSource dataSource)
	{
		this.dataSource = dataSource;
		jdbcTemplate = new JdbcTemplate(dataSource);
	}
	

	public List<ReportingUser> getReportingUsers(String reportId) {
		
		log.info("Executing query: "+ QueryConstants.GET_REPORTING_USERS);
		return jdbcTemplate.query(QueryConstants.GET_REPORTING_USERS, new Object[] {reportId} , new RowMapper<ReportingUser>() {

			public ReportingUser mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportingUser user = new ReportingUser();
				user.setUserEmail(rs.getString("USER_EMAIL"));
				user.setUserId(rs.getString("USER_ID"));
				user.setUserName(rs.getString("USER_NAME"));
				return user;
			}
			
		});
	}

	public ReportTemplate getReport(String reportId) {
		
		return jdbcTemplate.queryForObject(QueryConstants.GET_REPORT_TEMPLATE, new Object[] {reportId}, new RowMapper<ReportTemplate>() {

			public ReportTemplate mapRow(ResultSet rs, int rowNum) throws SQLException {
				ReportTemplate reportTemplate = new ReportTemplate();
				reportTemplate.setReportId(rs.getString("RPT_ID"));
				reportTemplate.setReportName(rs.getString("RPT_NAME"));
				reportTemplate.setTemplate(rs.getString("payload"));
				return reportTemplate;
			}
			
		});
	}


	public List<TableInfo> getReportingTables(String reportId) {
		
		log.info("Executing query: "+ QueryConstants.GET_TABLE_INFO_IN_ORDER);
		return jdbcTemplate.query(QueryConstants.GET_TABLE_INFO_IN_ORDER, new Object[] {reportId}, new RowMapper<TableInfo>() {

			@Override
			public TableInfo mapRow(ResultSet rs, int rowNum) throws SQLException {

				TableInfo table = new TableInfo(rs.getString("RLTD_TABLE"), rs.getString("SHEET_NAME"));
				table.setCustomQuery(rs.getString("CSTM_QRY"));
				return table;
			}
			
		});
	}


	public int getTableData(TableInfo table, ReportingUser user, List<List<String>> result) throws SQLException {
		
		PreparedStatement ps = null;
		Connection conn = null; 
		try
		{
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(table.getCustomQuery());
			if(table.getCustomQuery().contains("?"))
				ps.setString(1, user.getUserId());
			ResultSet rs = ps.executeQuery();
			int columnCount = rs.getMetaData().getColumnCount();
			populateColumnHeader(result, rs, columnCount);
			populateResult(result, rs , columnCount);
			return columnCount;
		}
		catch(SQLException ex)
		{
			log.error("Error while querying table: "+ex);
		}
		finally
		{
			if(ps!=null)
				ps.close();
			if(conn != null)
				conn.close();
			
		}
		return -1;
		
	}


	private void populateColumnHeader(List<List<String>> result, ResultSet rs, int columnCount) throws SQLException {
		List<String> header = new ArrayList<>();
		for(int i=1;i<=columnCount;i++)
		{
			header.add(rs.getMetaData().getColumnLabel(i));
		}
		result.add(header);
	}


	private void populateResult(List<List<String>> result, ResultSet rs, int columnCount) throws SQLException {
		
		while(rs.next())
		{
			List<String> row = new ArrayList<>();
			for(int i=1;i<=columnCount;i++)
			{
				row.add(rs.getString(i));
			}
			result.add(row);
		}
	}
	
	
}
