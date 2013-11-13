package com.womply.cassandradao.test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.womply.cassandradao.CassandraDAO;
import com.womply.cassandradao.Model;
import com.womply.cassandradao.annotations.Table;
import com.womply.cassandradao.cache.TransientPreparedStatementCache;

public class CassandraDAOTest extends AbstractKeyspaceTest {
	static final String CREATE_TEST_TABLE_CQL = String.format("create table %s.test (bliggity int, blah int, primary key(bliggity))", TEST_KEYSPACE);
	static final String TRUNCATE_TEST_TABLE_CQL = String.format("truncate %s.test", TEST_KEYSPACE);

	CassandraDAO<TestModel> cassandraDAO;

	@BeforeClass
	public static void setUpTable() throws Exception {
		session.execute(CREATE_TEST_TABLE_CQL);
	}
	
	@Before
	public void setUp() throws Exception {
		session.execute(TRUNCATE_TEST_TABLE_CQL);
		cassandraDAO = new CassandraDAO<>(TestModel.class, TEST_KEYSPACE, session, new TransientPreparedStatementCache(session));
	}
	
	@Test
	public void testSave() throws Exception {
		TestModel model = new TestModel(303, 909);
		cassandraDAO.save(model, ConsistencyLevel.ONE);
		
		TestModel persistedModel = cassandraDAO.first("bliggity = ?", new Object[] { model.getBliggity() },
				ConsistencyLevel.ONE);
		assertThat(persistedModel, notNullValue());
		assertThat(persistedModel.getBliggity(), equalTo(model.getBliggity()));
		assertThat(persistedModel.getBlah(), equalTo(model.getBlah()));
	}

	@Table(name="test")
	public static class TestModel extends Model {
		private int bliggity;
		private int blah;
		
		public TestModel() { }
		public TestModel(int bliggity, int blah) {
			this.bliggity = bliggity;
			this.blah = blah;
		}
		
		public int getBliggity() {
			return bliggity;
		}
		public void setBliggity(int bliggity) {
			this.bliggity = bliggity;
		}
		public int getBlah() {
			return blah;
		}
		public void setBlah(int blah) {
			this.blah = blah;
		}
	}
}
