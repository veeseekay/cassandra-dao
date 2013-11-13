package com.womply.cassandradao.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public abstract class AbstractKeyspaceTest {
	protected static final String TEST_KEYSPACE = "cassandra_dao_test";
	private static final String CREATE_TEST_KEYSPACE_CQL = String.format("create keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}", TEST_KEYSPACE);
	private static final String DROP_TEST_KEYSPACE_CQL = String.format("drop keyspace %s", TEST_KEYSPACE);
	
	static Session session;

	@BeforeClass
	public static void setUpSessionAndKeyspace() throws Exception {
		Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();
		
		session.execute(CREATE_TEST_KEYSPACE_CQL);
	}

	@AfterClass
	public static void dropKeyspace() throws Exception {
		session.execute(DROP_TEST_KEYSPACE_CQL);
	}

}
