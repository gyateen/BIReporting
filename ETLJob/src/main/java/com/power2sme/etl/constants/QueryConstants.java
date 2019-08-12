package com.power2sme.etl.constants;

public class QueryConstants {

	public static final String INSERT_LOG = "insert into p2s_ctrl.ods_job_log(job_id, run_id ,strt_time, end_time, qry_type, records_processed, status, error, records_selected) values(?,?, ?, ?,?,?,?,?,?)";
	public static final String INSERT_RUN = "insert into p2s_ctrl.ods_job_run_seq(status, strt_time) values(?, ?)";
	public static final String UPDATE_RUN = "update p2s_ctrl.ods_job_run_seq set status = ? , end_time = ? where run_id = ?";
	
	

}
