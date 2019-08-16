/*package com.power2sme.reporting.repository;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.power2sme.reporting.entity.ReportingUser;

@Repository
public class UserRepository {

	
	SessionFactory sessionFactory;
	
	@Autowired
	public UserRepository(SessionFactory sessionFactory)
	{
		this.sessionFactory = sessionFactory;
	}
	
	public ReportingUser getUserByEmail(String email)
	{
		return null;		
	}
	
	public void saveUser(ReportingUser user)
	{
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		session.save(user);
		session.getTransaction().commit();
		session.clear();
	}
}
*/