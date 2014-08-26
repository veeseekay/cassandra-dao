package com.womply.cassandradao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.womply.cassandradao.util.CassandraDAOUtil;

public enum CassandraManager {

	INSTANCE(CassandraDAOUtil.getSession());
	
	private Logger LOGGER = LoggerFactory.getLogger(CassandraManager.class);
	
	public Session session;

	CassandraManager(Session session) {
		this.session = session;
	}
	
	public void closeSession() {
		try {
			if (session != null) {
				session.getCluster().close();
				session = null;
			}
			else {
				LOGGER.error("Session not initialized...");
			}
		} catch (Exception e) {
			LOGGER.error("Exception occurred while closing Cassandra Session ",
					e);
		}
	}
}
