package com.womply.cassandradao.cache;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public abstract class PreparedStatementCache {
	protected Session session;
	
	public PreparedStatementCache(Session session) {
		this.session = session;
	}
	
	public abstract PreparedStatement getPreparedStatement(String cql);
}
