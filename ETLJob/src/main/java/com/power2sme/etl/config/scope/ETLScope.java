package com.power2sme.etl.config.scope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.Scope;

import com.power2sme.etl.request.ETLRequest;

public class ETLScope implements Scope {

	
	private static ThreadLocal<ETLRequest> etlRequest = new ThreadLocal<>();
	private Map<String, Object> scopedObjects = Collections.synchronizedMap(new HashMap<>());
	
	
	public static void put(ETLRequest request)
	{
		etlRequest.set(request);
		
	}
	
	
	@Override
	public Object get(String name, ObjectFactory<?> objectFactory) {
		
		if(name.equalsIgnoreCase("scopedTarget.ETLRequest"))
			return etlRequest.get();
		if(!scopedObjects.containsKey(name))
		{
			Object o = objectFactory.getObject();
			scopedObjects.put(name, o);
			return o;
		}
		return scopedObjects.get(name);
	}

	@Override
	public Object remove(String name) {
		return scopedObjects.remove(name);
	}

	@Override
	public void registerDestructionCallback(String name, Runnable callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object resolveContextualObject(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConversationId() {
		// TODO Auto-generated method stub
		return null;
	}

}
