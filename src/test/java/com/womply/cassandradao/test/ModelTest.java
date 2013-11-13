package com.womply.cassandradao.test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.womply.cassandradao.CassandraDAO;
import com.womply.cassandradao.CassandraDAOException;
import com.womply.cassandradao.ColumnMapping;
import com.womply.cassandradao.Model;
import com.womply.cassandradao.TableMapping;
import com.womply.cassandradao.annotations.Table;
import com.womply.cassandradao.cache.TransientPreparedStatementCache;

public class ModelTest extends AbstractKeyspaceTest {
	static final String CREATE_USER_TABLE_CQL = String.format("create table %s.users (username text, first_name text, last_name text, primary key(username))", TEST_KEYSPACE);
	static final String TRUNCATE_USER_TABLE_CQL = String.format("truncate %s.users", TEST_KEYSPACE);

	protected UserDao dao;

	@BeforeClass
	public static void setUpTable() throws Exception {
		session.execute(CREATE_USER_TABLE_CQL);
	}
	
	@Before
	public void setUp() throws Exception {
		session.execute(TRUNCATE_USER_TABLE_CQL);
		
		dao = new UserDao();
	}
	
	@Test
	public void testColumnStatesForNewRecord() throws Exception {
		User model = new User();
		assertThat(model.getChangedColumnNames(dao).isEmpty(), is(true));
		
		model.setUsername("ill_logic");
		assertThat(model.getChangedColumnNames(dao), containsInAnyOrder(new String[] { "username" }));
		
		model.setFirstName("Brent");
		assertThat(model.getChangedColumnNames(dao), containsInAnyOrder(new String[] { "username", "first_name" }));
		
		dao.save(model, ConsistencyLevel.ONE);
		assertThat(model.getChangedColumnNames(dao).isEmpty(), is(true));
		
		model.setLastName("Theisen");
		assertThat(model.getChangedColumnNames(dao), containsInAnyOrder(new String[] { "last_name" }));
	}
	
	@Test
	public void testColumnStatesForPersistedRecord() throws Exception {
		User model = new User("ill_logic", "Brent", null);
		dao.save(model, ConsistencyLevel.ONE);
		model = dao.first("username = ?", new Object[] { model.getUsername() }, ConsistencyLevel.ONE);
		
		model.setLastName("Theisen");
		assertThat(model.getChangedColumnNames(dao), containsInAnyOrder(new String[] { "last_name" }));
	}
	
	@Table
	public static class User extends Model {
		private String username;
		private String firstName;
		private String lastName;
		
		public User() { }
		public User(String username, String firstName, String lastName) {
			this.username = username;
			this.firstName = firstName;
			this.lastName = lastName;
		}
		
		public List<String> getChangedColumnNames(UserDao dao) throws CassandraDAOException {
			List<String> changedColumnNames = new ArrayList<String>();
			for(ColumnMapping colMapping : getChangedValuesByColumnMappings(dao.getModelMapping()).keySet()) {
				changedColumnNames.add(colMapping.getName());
			}
			return changedColumnNames;
		}
		
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getFirstName() {
			return firstName;
		}
		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}
		public String getLastName() {
			return lastName;
		}
		public void setLastName(String lastName) {
			this.lastName = lastName;
		}
	}
	
	private class UserDao extends CassandraDAO<User> {

		protected UserDao() throws CassandraDAOException {
			super(User.class, TEST_KEYSPACE, session, new TransientPreparedStatementCache(session));
		}

		public TableMapping<? extends Model> getModelMapping() {
			return tableMapping;
		}
	}

}
