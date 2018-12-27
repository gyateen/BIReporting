package com.power2sme.reporting.constants;

public class QueryConstants {

	public static final String GET_REPORTING_USERS = "select * from rpt_users_info where user_id in (select user_id from rpt_users_map where rpt_id = ?)";

	public static final String GET_REPORT_TEMPLATE = "select * from rpt_tables where rpt_id = ?";

	//public static final String GET_TABLE_DATA = "select * from TABLE_NAME where manager_id = ?";
	
	public static final String GET_TABLE_INFO_IN_ORDER = "select * from rpt_tables_map where rpt_id = ? order by s_no";

}
