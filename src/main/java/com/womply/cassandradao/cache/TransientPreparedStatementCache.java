package com.womply.cassandradao.cache;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class TransientPreparedStatementCache extends PreparedStatementCache {

	public TransientPreparedStatementCache(Session session) {
		super(session);
	}

	@Override
	public PreparedStatement getPreparedStatement(String cql) {
		return session.prepare(cql);
	}

}
