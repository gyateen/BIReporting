package com.power2sme.etl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.google.common.collect.Lists;
import com.power2sme.etl.jdbc.JdbcTemplateMapper;
import com.power2sme.etl.mappers.CustomDataTypeMapper;
import com.power2sme.etl.utils.BatchCounter;
import com.power2sme.etl.utils.ETLUtils;
import com.power2sme.etl.constants.ETLConstants;
import com.power2sme.etl.constants.QueryConstants;
import com.power2sme.etl.dao.ETLRowMapper;
import com.power2sme.etl.entity.ETLColumn;
import com.power2sme.etl.entity.ETLColumnHeader;
import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.entity.ETLRowHeader;
import com.power2sme.etl.exceptions.ETLFailureException;
import com.power2sme.etl.input.ETLReader;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class ETLDao {

	public static final String ETL_METADATA_DB = "P2S_RPT";
	JdbcTemplateMapper jdbcTemplateMapper;
	ExecutorService executorService = Executors.newFixedThreadPool(10);
	ExecutorService etlReaderExec = Executors.newFixedThreadPool(10);
	
	
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

	public int insertETLBatch(String insertQuery,  List<ETLRecord> etlRecords, boolean isError) throws SQLException
	{
		
		Connection conn = null;
		try
		{
			DataSource dataSource = jdbcTemplateMapper.getJdbcTemplate(targetDB).getDataSource();
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			List<List<ETLRecord>> batches = Lists.partition(etlRecords, 100);
			BatchCounter executeBatchCount = new BatchCounter(0);
			for(List<ETLRecord> executeBatch: batches)
			{
				
		//		log.info("Executing new batch");

				executorService.submit(new ProcessBatch(executeBatch, insertQuery, conn, executeBatchCount, isError));
				
			}
				
			while(executeBatchCount.getCounter() != batches.size())
			{
				synchronized(conn)
				{
					try
					{
						if(executeBatchCount.getCounter() != batches.size())
							conn.wait();
					}
					catch(InterruptedException ex)
					{
						log.info("Thread waiting on connection object interrupted: "+conn);
					}
				}
				
				
			}
			log.info(new Date()+" committing transaction");
			conn.commit();
			return executeBatchCount.getTotalRecords();
		}
		catch(SQLException ex)
		{
			log.error("Exception while inserting"+ex);
			return 0;
		}
		finally
		{
			if(conn !=null)
				conn.close();
		}
			
		
	}

	
	/*
	 * public List<ETLQuery> fetchqueriesForJob(Integer jobId) { JdbcTemplate
	 * jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(ETL_METADATA_DB); String
	 * query = "select * from  P2S_RPT.ODS_JOB_METADATA where job_id = ?"; return
	 * jdbcTemplate.query(query,new Object[] {jobId}, new RowMapper<ETLQuery>() {
	 * 
	 * @Override public ETLQuery mapRow(ResultSet rs, int rowNum) throws
	 * SQLException { ETLQuery etlQuery = new ETLQuery();
	 * etlQuery.setSrcDB(rs.getString("src_db"));
	 * etlQuery.setSrcTable(rs.getString("src_tbl"));
	 * etlQuery.setSrcQuery(rs.getString("qry"));
	 * etlQuery.setTargetTable(rs.getString("tgt_tbl"));
	 * etlQuery.setTargetDB(rs.getString("tgt_schema")); return etlQuery; }
	 * 
	 * }); }
	 */

	public ETLReader<ETLRecord> fetchETLReader(String srcQuery, ETLRowHeader header)
	{
		Connection conn;
		try
		{
			DataSource dataSource = jdbcTemplateMapper.getJdbcTemplate(srcDB).getDataSource();
			conn = dataSource.getConnection();
			PreparedStatement ps = conn.prepareStatement(srcQuery);
			ResultSet rs = ps.executeQuery();
			FutureETLReader etlReader = new FutureETLReader(conn, ps, rs);
			etlReaderExec.execute(etlReader);
			return etlReader;
		}
		catch(SQLException ex)
		{
			throw new ETLFailureException(ex.getMessage());
		}
		
		
	}
	
	public List<ETLRecord> fetchETLRecords(String srcQuery, ETLRowHeader header)
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
				
				
				return ETLUtils.mapResultSetToETLRecord(rs);
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
	
	public void truncateTable(String table,  String schema)
	{
		
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(db);
		try
		{
			log.info(new Date() + " Truncating table: " + table);
			jdbcTemplate.execute(constructTruncateTableQuery(table, schema));
		}
		catch(RuntimeException ex)
		{
			throw new ETLFailureException(ex.getMessage());
		}
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
	
	public void insertLog(ETLLog etlLog)
	{
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(ETLConstants.LOG_DATABASE);
		String insertQuery = QueryConstants.INSERT_LOG;
		try
		{
		jdbcTemplate.update(insertQuery, new PreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				
				ps.setInt(1, etlLog.jobId);
				ps.setLong(2,  etlLog.runId);
				ps.setTimestamp(3, new java.sql.Timestamp(etlLog.getStartTime().getTime()));
				ps.setTimestamp(4,  new java.sql.Timestamp(etlLog.getEndTime().getTime()));
				ps.setString(5, etlLog.getQueryType());
				ps.setInt(6,  etlLog.getRecordsProcessed());
				ps.setString(7, etlLog.getStatus());
				ps.setString(8, etlLog.getError());
			}
			
		});
		}
		catch(DataAccessException ex)
		{
			log.error("Error while inserting etl log: "+ex);
		}
	}
	
	public long insertRunForJob()
	{
		KeyHolder keyHolder = new GeneratedKeyHolder();
		JdbcTemplate jdbcTemplate = jdbcTemplateMapper.getJdbcTemplate(ETLConstants.LOG_DATABASE);
		String insertQuery = QueryConstants.INSERT_RUN;
		jdbcTemplate.update(new PreparedStatementCreator()
				{

					@Override
					public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
						PreparedStatement ps = con.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
						ps.setString(1, ETLConstants.INIITAL_JOB_STATUS);
						ps.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
						return ps;
					}
			
				}, keyHolder);
		
		
		return keyHolder.getKey().longValue();
	}
	
	private String constructTruncateTableQuery(String table, String db)
	{
		StringBuilder query = new StringBuilder("truncate table ");
		query.append(db).append(".").append(table);
		return query.toString();
	}
}
