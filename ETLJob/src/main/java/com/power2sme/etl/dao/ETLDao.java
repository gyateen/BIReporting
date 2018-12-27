package com.power2sme.etl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.power2sme.etl.jdbc.JdbcTemplateMapper;
import com.power2sme.etl.utils.BatchCounter;
import com.power2sme.etl.dao.ETLRowMapper;
import com.power2sme.etl.entity.ETLColumn;
import com.power2sme.etl.entity.ETLColumnHeader;
import com.power2sme.etl.entity.ETLQuery;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class ETLDao {

	public static final String ETL_METADATA_DB = "P2S_RPT";
	JdbcTemplateMapper jdbcTemplateMapper;
	ExecutorService executorService = Executors.newFixedThreadPool(5);
	
	@Autowired
	@Qualifier("odsdatasource")
	DataSource odsDataSource;
	
	@Autowired
	public ETLDao(JdbcTemplateMapper jdbcTemplateMapper)
	{
		this.jdbcTemplateMapper = jdbcTemplateMapper;
	}
	
	
	public void insertETLRecords(String insertQuery, String targetDB,List<ETLRecord> etlRecords, ETLRowHeader header)
	{
		
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(targetDB);
		jdbcTemplate.batchUpdate(insertQuery, new BatchPreparedStatementSetter()
				{

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ETLRecord row = etlRecords.get(i);
						for(ETLColumn column: row.getColumns())
						{
							try
							{
							ps.setObject(column.getPosition(), column.getVal());
							}
							catch(Exception ex)
							{
								log.info("Exception while inserting"+ex);
							}
						}
						
					}

					@Override
					public int getBatchSize() {
						return 1000;
					}
			
				});
		
		throw new RuntimeException("throw error to rollback");
	}

	public void insertETLBatch(String insertQuery, String targetDB,List<ETLRecord> etlRecords, ETLRowHeader header) throws SQLException
	{
		
		Connection conn = null;
		try
		{
			conn = odsDataSource.getConnection();
			conn.setAutoCommit(false);
			List<List<ETLRecord>> batches = Lists.partition(etlRecords, 100);
			BatchCounter executeBatchCount = new BatchCounter(0);
			for(List<ETLRecord> executeBatch: batches)
			{
				
	//			log.info("Executing new batch");
				executorService.execute(new ProcessBatch(executeBatch, insertQuery, conn, executeBatchCount));
			}
				
			while(executeBatchCount.getCounter() != batches.size())
			{
				synchronized(conn)
				{
					conn.wait();
				}
				
				
			}
			log.info(new Date()+" committing transaction");
				conn.commit();
		}
		catch(Exception ex)
		{
			log.info("Exception while inserting"+ex);
			
			
		}
		finally
		{
			if(conn !=null)
				conn.close();
		}
		
		
		
		
	}

	
	public List<ETLQuery> fetchqueriesForJob(Integer jobId)
	{
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(ETL_METADATA_DB);
		String query = "select * from  P2S_RPT.ODS_JOB_METADATA where job_id = ?";
		return jdbcTemplate.query(query,new Object[] {jobId}, new RowMapper<ETLQuery>()
				{

					@Override
					public ETLQuery mapRow(ResultSet rs, int rowNum) throws SQLException {
						ETLQuery etlQuery = new ETLQuery();
						etlQuery.setSrcDB(rs.getString("src_db"));
						etlQuery.setSrcTable(rs.getString("src_tbl"));
						etlQuery.setSrcQuery(rs.getString("qry"));
						etlQuery.setTargetTable(rs.getString("tgt_tbl"));
						etlQuery.setTargetDB(rs.getString("tgt_schema"));
						return etlQuery;
					}
			
				});
	}

	public List<ETLRecord> fetchETLRecords(String srcQuery,String srcDB, ETLRowHeader header)
	{
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(srcDB);
		return jdbcTemplate.query(srcQuery, new ETLRowMapper<ETLRecord>(header) {
			
			@Override
			public ETLRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
			//	log.info("fetching record:" +rowNum);
				if(!isheaderPopulated())
				{
					this.populateColumnHeader(rs);
					this.headerPopulated();
				}
				
				ETLRecord record = new ETLRecord();
				List<ETLColumn> columns = new ArrayList<>();
				for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
				{
					ETLColumn column = new ETLColumn();
					column.setPosition(i);
					column.setVal(rs.getObject(i));
					columns.add(column);
				}
				record.setColumns(columns);
				return record;
			}


			@Override
			public void populateColumnHeader(ResultSet rs) throws SQLException{
				ETLRowHeader header = this.getRowHeader();
				List<ETLColumnHeader> columnHeaders = new ArrayList<>(); 
				for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
				{
					ETLColumnHeader columnHeader = new ETLColumnHeader();
					columnHeader.setName(rs.getMetaData().getColumnName(i));
					columnHeader.setType(rs.getMetaData().getColumnType(i));
					columnHeader.setLength(rs.getMetaData().getPrecision(i));
					columnHeaders.add(columnHeader);
				}
				header.setColumnHeaders(columnHeaders);
				
			}
			
		});
	}
	
	public void truncateTable(String table, String db)
	{
		
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(db);
		jdbcTemplate.execute(constructTruncateTableQuery(table, db));
	}
	
	
	public void dropIndex(String table, String db)
	{
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(db);
		jdbcTemplate.execute("drop index idx_Cust_Ledg_Entry_cust_no on P2S_RPT.BS_NAV_CUST_LEDG_ENTRY;");
		jdbcTemplate.execute("drop index idx_Cust_Ledg_Entry_doc_no on P2S_RPT.BS_NAV_CUST_LEDG_ENTRY;");
	}
	
	public void createIndex(String table, String db)
	{
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(db);
		jdbcTemplate.execute("create index idx_Cust_Ledg_Entry_cust_no on P2S_RPT.BS_NAV_CUST_LEDG_ENTRY(`Customer No_`);");
		jdbcTemplate.execute(" ");
	}
	
	private String constructTruncateTableQuery(String table, String db)
	{
		StringBuilder query = new StringBuilder("delete from ");
		query.append(db).append(".").append(table);
		return query.toString();
	}
}
