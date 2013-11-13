package com.womply.cassandradao.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class MapPreparedStatementCache extends PreparedStatementCache {
	private Map<Integer, PreparedStatement> preparedStatementHashCodes = new ConcurrentHashMap<Integer, PreparedStatement>();
	
	public MapPreparedStatementCache(Session session) {
		super(session);
	}

	public PreparedStatement getPreparedStatement(String cql) {
		int hashCode = cql.hashCode();
		PreparedStatement stmt = preparedStatementHashCodes.get(hashCode);
		if(stmt == null) {
			stmt = session.prepare(cql);
			preparedStatementHashCodes.put(hashCode, stmt);
		}
		
		return stmt;
	}
}
