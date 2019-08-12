package com.power2sme.ETLJob;

import static org.junit.Assert.fail;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.junit.Test;

import com.power2sme.etl.entity.ETLRecord;
import com.power2sme.etl.exceptions.ETLFailureException;
import com.power2sme.etl.input.ETLReader;
import com.power2sme.etl.job.ETLJob;
import com.power2sme.etl.routines.DateCompareRoutine;
import com.power2sme.etl.routines.DomainCheckRoutine;
import com.power2sme.etl.routines.NULL_CHECK_ROUTINE;
import com.power2sme.etl.rules.ETLRule;
import com.power2sme.etl.rules.RuleType;

public class ETLTest {

	
	@Test
	public void testDateCompareRoutine()
	{
		/*
		 * Object firstDate = new Date(); Object secondDate = null;
		 * DateCompareRoutine.compareDate(firstDate, secondDate);
		 */	}
	
	
	@Test
	public void testJob() {
		
		try
		{
			Properties context = getTestContext();
			long runId = ETLJob.initJob(context);
			int count = 0;
			while(count <2)
			{
				count++;
			
			ETLReader etlReader = ETLJob.runInputJob(Long.valueOf(runId), context);
			
	//		ETLReader<ETLRecord> staging =  ETLJob.runStagingJob(runId, context, etlReader, getTestJexlContext(), getTestRuleMap(), getTestDomainMap());
	
			ETLJob.runOutputJob(runId,etlReader, context);
			}
			fail();
		}
		catch(ETLFailureException ex)
		{
			
		}
	}
	
	
	
	public Properties getTestContext()
	{
		Properties contextProp = new Properties();
		
		
		setTestContextForLocal(contextProp);
		
		contextProp.setProperty("REPORTING_DATE_FORMAT", "yyyy-MM-dd");
		contextProp.setProperty("JOB_ID", "8");
		contextProp.setProperty("QRY", getTestQuery());
		
		contextProp.setProperty("JOB_TYPE", "test_job");
		return contextProp;
	}
	
	public static void setTestContextForSrcODS(Properties contextProp)
	{
		contextProp.setProperty("SRC_DATABASE", "LOCAL");
		contextProp.setProperty("SRC_DB_SERVER", "MYSQL");
		contextProp.setProperty("SRC_PASSWORD", "^C,qaJ36B");
		contextProp.setProperty("SRC_USER", "team_db_wrhouse");
		contextProp.setProperty("SRC_URL", "jdbc:mysql://192.168.1.14:3306");
		contextProp.setProperty("TGT_SCHEMA", "p2s_stg");
		contextProp.setProperty("TGT_TABLE", "stg_crm_accounts");
		contextProp.setProperty("TGT_STAGE_TABLE", "stg_crm_accounts_rjct");
	}
	
	
	public static void setTestContextForLocal(Properties contextProp)
	{
		setTestContextForSrcLocal(contextProp);
		setTestContextForTargetLocal(contextProp);
	}
	
	public static void setTestContextForTargetLocal(Properties contextProp) {
		contextProp.setProperty("TGT_SCHEMA", "ods_test");
		contextProp.setProperty("TGT_TABLE", "employee_c");
		contextProp.setProperty("TGT_STAGE_TABLE", "employee_c_stg");
		contextProp.setProperty("TGT_DATABASE", "ODS");
		contextProp.setProperty("TGT_DB_SERVER", "MYSQL");
		contextProp.setProperty("TGT_PASSWORD", "root");
		contextProp.setProperty("TGT_USER", "root");
		contextProp.setProperty("TGT_URL", "jdbc:mysql://localhost:3306");
		
	}


	public static void setTestContextForSrcLocal(Properties contextProp)
	{
		contextProp.setProperty("SRC_DATABASE", "LOCAL");
		contextProp.setProperty("SRC_SCHEMA", "ods_test");
		contextProp.setProperty("SRC_TABLE", "test_table");
		contextProp.setProperty("SRC_DB_SERVER", "MYSQL");
		contextProp.setProperty("SRC_PASSWORD", "root");
		contextProp.setProperty("SRC_USER", "root");
		contextProp.setProperty("SRC_URL", "jdbc:mysql://localhost:3306");
		
	}

	
	public static void setTestContextForSrcNAV(Properties contextProp)
	{
		contextProp.setProperty("SRC_DATABASE", "NAV");
		contextProp.setProperty("SRC_DB_SERVER", "MSSQL");
		contextProp.setProperty("SRC_PASSWORD", "zsa!1234");
		contextProp.setProperty("SRC_USER", "shweta");
		contextProp.setProperty("SRC_URL", "jdbc:sqlserver://103.25.172.167:1433");
		contextProp.setProperty("TGT_SCHEMA", "p2s_bs");
		contextProp.setProperty("TGT_TABLE", "bs_nav_posted_str_order_line_details");
	}
	
	public static void setTestContextForSrcCRM(Properties contextProp)
	{
		contextProp.setProperty("SRC_DATABASE", "CRM");
		contextProp.setProperty("SRC_DB_SERVER", "MYSQL");
		contextProp.setProperty("SRC_PASSWORD", "wn!-X6w+:");
		contextProp.setProperty("SRC_USER", "team_suitecrm_r");
		contextProp.setProperty("SRC_URL", "jdbc:mysql://103.25.172.187:3306?zeroDateTimeBehavior=convertToNull");
		contextProp.setProperty("TGT_SCHEMA", "p2s_bs");
		contextProp.setProperty("TGT_TABLE", "bs_crm_calls");
	}
	
	private static JexlContext getTestJexlContext()
	{
		JexlContext jc = new MapContext();  
		jc.set("NULL_CHECK_ROUTINE", NULL_CHECK_ROUTINE.class);
		jc.set("DomainCheckRoutine", DomainCheckRoutine.class);
		jc.set("DateCompareRoutine", DateCompareRoutine.class);
		return jc;
	}
	
	private static Map<String, ETLRule> getTestRuleMap()
	{
		Map<String,ETLRule> ruleMap = new HashMap<>();
		ETLRule rule1 = new ETLRule("qc001", "NULL_CHECK_ROUTINE.null_check(row.entry_no)== true", RuleType.ERROR);
		ETLRule rule2 = new ETLRule("qc002", "DomainCheckRoutine.DomainCheckRoutinee(row.document_type,document_type_arr)==false", RuleType.ERROR);
		ETLRule rule3 = new ETLRule("qc003", "NULL_CHECK_ROUTINE.null_check(row.posting_date)==true", RuleType.ERROR);
		ETLRule rule4 = new ETLRule("qc004", "NULL_CHECK_ROUTINE.null_check(row.document_no)==true", RuleType.ERROR);
		ETLRule rule5 = new ETLRule("qc005", "(NULL_CHECK_ROUTINE.null_check(row.gen_bus_pos_c)==true)&&(DateCompareRoutine.compareDate(row.date_entered,valid_date_arr[0])==1)", RuleType.ERROR);
		ETLRule rule6 = new ETLRule("qc006", "(NULL_CHECK_ROUTINE.null_check(row.pan_c)==true)&&(DateCompareRoutine.compareDate(row.date_entered,valid_date_arr[0])==1)", RuleType.ERROR);
		ETLRule rule7 = new ETLRule("qc007", "DateCompareRoutine.compareDate(row.date_entered,valid_date_arr[0])==1", RuleType.ERROR);
//		ruleMap.put("qc001", rule1);
//		ruleMap.put("qc002", rule2);
//		ruleMap.put("qc003", rule3);
//		ruleMap.put("qc004", rule4);
	//	ruleMap.put("qc005", rule5);
		ruleMap.put("qc007", rule7);
		return ruleMap;
	}
	
	private static Map<String, String[]> getTestDomainMap()
	{
		Map<String,String[]> domainMap = new HashMap<>();
		
		domainMap.put("document_type_arr", new String[] {"0", "1", "2", "3","4","5","6"} );
		domainMap.put("valid_date_arr", "2018-01-01 00:00:00".split(","));
		return domainMap;
	}
	
	private static String getLocalTestQuery()
	{
		String query = "select id, name, age from ods_test.employee";
		return query;
	}
	
	private static String getTestQuery()
	{
	//	String query = "select[timestamp],[Entry No_],[Cust_ Ledger Entry No_],[Entry Type],[Posting Date],[Document Type],[Document No_],[Amount],[Amount (LCY)],[Customer No_],[Currency Code],[User ID],[Source Code],[Transaction No_],[Journal Batch Name],[Reason Code],[Debit Amount],[Credit Amount],[Debit Amount (LCY)],[Credit Amount (LCY)],[Initial Entry Due Date],[Initial Entry Global Dim_ 1],[Initial Entry Global Dim_ 2],[Gen_ Bus_ Posting Group],[Gen_ Prod_ Posting Group],[Use Tax],[VAT Bus_ Posting Group],[VAT Prod_ Posting Group],[Initial Document Type],[Applied Cust_ Ledger Entry No_],[Unapplied],[Unapplied by Entry No_],[Remaining Pmt_ Disc_ Possible],[Max_ Payment Tolerance],[Tax Jurisdiction Code],[Application No_],[TDS Nature of Deduction],[TDS Group],[Total TDS_TCS Incl_ SHECESS],[TCS Nature of Collection],[TCS Type],[Payment Type],[Payment Terms Code],[RMS No_],[NBFC ID],[Last RMS Sent Date],[Sent to RMS System],[Payment Method Code],[Old NBFC ID],[Old Payment Method Code] ,    SYSDATETIME()  from [Bebb New].[dbo].[BEBB_India$Detailed Cust_ Ledg_ Entry];";
		String crmQuery = "SELECT a.id ,a.name ,a.date_entered ,a.date_modified ,a.modified_user_id ,a.created_by ,a.deleted ,a.assigned_user_id ,a.nav_accountid ,ac.pan_c ,ac.cst_number_c ,ac.tin_c ,ac.ecc_number_c ,ac.cin_c ,ac.sme_id_c ,ac.cust_post_grp_c ,ac.gen_bus_pos_c ,UPPER(ac.business_type_c) AS business_type_c ,ac.credit_days_c ,ac.credit_limit_c ,ac.delayed_interest_c ,ac.interest_c ,UPPER(ac.type_c) AS type_c,UPPER(ac.cus_gst_cus_type_c) AS cus_gst_cus_type_c,UPPER(ac.cus_gst_reg_type_c) AS cus_gst_reg_type_c,UPPER(ac.creditrequest_c) AS creditrequest_c,ac.status_c ,UPPER(ac.cluster_name_c) AS cluster_name_c,UPPER(ac.kyc_verification_status_c) AS kyc_verification_status_c,UPPER(ac.kyc_verification_date_c) AS kyc_verification_date_c,ac.sync_with_nav_c ,ac.insuredlimit_c ,UPPER(ac.insurancestatus_c) AS insurancestatus_c,UPPER(ac.ins_reason_c) AS ins_reason_c,ac.insur_app_status_c ,UPPER(ac.totaloutstanding_c) AS totaloutstanding_c,ac.wallet_status_c ,ac.bgcoc_c,ac.sdcoc_c,ac.contact_id_c ,ac.contact_phone_c ,ac.account_category_c ,ac.last_rfq_date_c ,NOW() FROM p2s_bs.bs_crm_accounts a JOIN p2s_bs.bs_crm_accounts_cstm ac ON (a.id = ac.id_c) WHERE a.deleted = 0 AND a.date_entered >= (select date_val from p2s_ctrl.ods_date_ref where src_nm='CRM')";
		
		String navQuery = "select[timestamp],[Type],[Calculation Order],[Document Type],[Invoice No_],[Item No_],[Line No_],[Tax_Charge Type],[Tax_Charge Group],[Tax_Charge Code],[Structure Code],[Calculation Type],[Calculation Value],[Quantity Per],[Loading on Inventory],[_ Loading on Inventory],[Payable to Third Party],[Third Party Code],[Account No_],[Base Formula],[Base Amount],[Amount],[Include Base],[Include Line Discount],[Include Invoice Discount],[Charge Basis],[Amount (LCY)],[Header_Line],[Manually Changed],[LCY],[Available for VAT input],[CVD],[CVD Payable to Third Party],[CVD Third Party Code],[Price Inclusive of Tax],[Include PIT Calculation],[Include in TDS Base],[Inc_ GST in TDS Base] ,    SYSDATETIME()  from [Bebb New].[dbo].[BEBB_India$Posted Str Order Line Details];";
		return getLocalTestQuery();
	}

}
