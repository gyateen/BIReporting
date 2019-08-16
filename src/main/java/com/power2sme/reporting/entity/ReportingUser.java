package com.power2sme.reporting.entity;

/*import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
*/
import lombok.Data;

@Data
//@Entity
//@Table(name="rpt_users_info")
public class ReportingUser {

	//@Id
	//@Column(name = "user_id")
	String userId;
	//@Column(name = "user_name")
	String userName;
//	@Column(name = "user_email")
	String userEmail;
	
}
