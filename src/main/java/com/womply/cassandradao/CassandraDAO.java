package com.womply.cassandradao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.womply.cassandradao.cache.MapPreparedStatementCache;
import com.womply.cassandradao.cache.PreparedStatementCache;

public class CassandraDAO <T extends Model> {
	private static final Logger logger = LogManager.getLogger(CassandraDAO.class);
	
	protected static final Map<Class<? extends Model>, TableMapping<? extends Model>> tableMappings =
			new HashMap<Class<? extends Model>, TableMapping<? extends Model>>();
	
	protected PreparedStatementCache preparedStatementCache;
	protected TableMapping<T> tableMapping;
	protected Class<T> modelClass;
	private Session session;

	public CassandraDAO(Class<T> modelClass, String keyspaceName, Session session) throws CassandraDAOException {
		this(modelClass, keyspaceName, null, session, new MapPreparedStatementCache(session));
	}
	
	public CassandraDAO(Class<T> modelClass, String keyspaceName, Session session,
			PreparedStatementCache preparedStatementCache) throws CassandraDAOException {
		this(modelClass, keyspaceName, null, session, preparedStatementCache);
	}
	
	@SuppressWarnings("unchecked")
	public CassandraDAO(Class<T> modelClass, String keyspaceName, String tableName, Session session,
			PreparedStatementCache preparedStatementCache) throws CassandraDAOException {
		
		this.session = session;
		this.preparedStatementCache = preparedStatementCache;
		this.modelClass = modelClass;
		synchronized(tableMappings) {
			this.tableMapping = (TableMapping<T>) tableMappings.get(modelClass);
			if(tableMapping == null) {
				tableMapping = new TableMapping<T>(modelClass, keyspaceName,
						tableName, this.session.getCluster());
				tableMappings.put(modelClass, tableMapping);
			}
		}
	}
	
	public T first(String whereClause, ConsistencyLevel cl) throws CassandraDAOException {
		return first(whereClause, null, cl);
	}
	public T first(String whereClause, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		ResultSet<T> rs = find(whereClause, 1, values, cl);
		T first = null;
		Iterator<T> it = rs.iterator();
		if(it.hasNext())
			first = it.next();

		return first;
	}
	
	public ResultSet<T> findAll(ConsistencyLevel cl) throws CassandraDAOException {
		return findAll(null, cl);
	}
	
	public ResultSet<T> findAll(int pageSize, ConsistencyLevel cl) throws CassandraDAOException {
		return findAll(pageSize, null, cl);
	}
	
	public ResultSet<T> findAll(String selectClause, ConsistencyLevel cl) throws CassandraDAOException {
		return findAll(null, selectClause, cl);
	}

	public ResultSet<T> findAll(Integer pageSize, String selectClause, ConsistencyLevel cl) throws CassandraDAOException {
		return select(selectClause, null, null, null, cl);
	}
	
	public ResultSet<T> find(String whereClause, ConsistencyLevel cl) throws CassandraDAOException {
		return find(whereClause, null, cl);
	}
	
	public ResultSet<T> find(Integer limit, ConsistencyLevel cl) throws CassandraDAOException {
		return find(null, limit, null, cl);
	}

	public ResultSet<T> find(String whereClause, Object value, ConsistencyLevel cl) throws CassandraDAOException {
		return find(whereClause, new Object[] { value }, cl);
	}

	public ResultSet<T> find(String whereClause, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		return find(null, whereClause, null, values, cl);
	}

	public ResultSet<T> find(String whereClause, Integer limit, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		return find(null, whereClause, limit, values, cl);
	}

	public ResultSet<T> find(String selectClause, String whereClause, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		return find(selectClause, whereClause, null, values, cl);
	}

	public ResultSet<T> find(String selectClause, String whereClause, Integer limit, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		return select(selectClause, whereClause, limit, values, cl);
	}
	
	protected String selectCql(String selectClause, String whereClause, Integer limit, Object[] values) {
		StringBuffer sb = new StringBuffer("SELECT ");
		if(selectClause == null)
			sb.append("*");
		else
			sb.append(selectClause);
		
		sb.append(" FROM ");
		
		sb.append(tableMapping.getKeyspaceName());
		sb.append('.');
		sb.append(tableMapping.getTableName());
		
		if(whereClause != null) {
			sb.append(" WHERE ");
			sb.append(whereClause);
		}
		
		if(limit != null)
			sb.append(String.format(" LIMIT %d", limit));
		
		return sb.toString();
	}
	
	protected ResultSet<T> select(String selectClause, String whereClause, Integer limit, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		String cql = selectCql(selectClause, whereClause, limit, values);
		return new ModelResultSet<>(tableMapping, execute(cql, values, cl));
	}
	
	public void save(T model, ConsistencyLevel cl) throws CassandraDAOException {
		insert(model, cl);
	}
	
	private void insert(T model, ConsistencyLevel cl) throws CassandraDAOException {
		// TODO The two maps could be consolidated in to a single call to avoid overlapping getter calls
		Map<ColumnMapping, Object> valuesToSaveByColumnMappings = model.getChangedValuesByColumnMappings(tableMapping);
		if(valuesToSaveByColumnMappings.isEmpty())
			return;
		
		for(Map.Entry<ColumnMapping, Object> entry : model.getPrimaryKeyValuesByColumnMapping(tableMapping).entrySet()) {
			valuesToSaveByColumnMappings.put(entry.getKey(), entry.getValue());
		}
		
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		sb.append(tableMapping.getKeyspaceName());
		sb.append('.');
		sb.append(tableMapping.getTableName());
		sb.append(" (");
		
		int size = valuesToSaveByColumnMappings.size();
		String[] columnNames = new String[size];
		Object[] values = new Object[size];
		String[] placeholders = new String[size];

		int index = 0;
		for(Map.Entry<ColumnMapping, Object> pair : valuesToSaveByColumnMappings.entrySet()) {
			ColumnMapping columnMapping = pair.getKey();
			Object value = pair.getValue();
			
			columnNames[index] = columnMapping.getName();
			values[index] = value;
			placeholders[index] = "?";
			index++;
		}
			
		sb.append(StringUtils.join(columnNames, ", "));
		sb.append(") VALUES (");
		sb.append(StringUtils.join(placeholders, ", "));
		sb.append(')');
		
		execute(sb.toString(), values, cl);

		// TODO Another round of getter reflection calls could be mitigated if the changed values were passed here
		model.updateColumnStates();
	}
	
	public void deleteAll(String whereClause, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		deleteAll(whereClause, null, values, cl);
	}
	
	public void deleteAll(String whereClause, String columnName, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE ");
		if(columnName != null) {
			sb.append(columnName);
			sb.append(' ');
		}
		sb.append("FROM ");
		sb.append(tableMapping.getKeyspaceName());
		sb.append('.');
		sb.append(tableMapping.getTableName());
		sb.append(" WHERE ");
		sb.append(whereClause);
		execute(sb.toString(), values, cl);
	}
	
	public void truncate(ConsistencyLevel cl) throws CassandraDAOException {
		StringBuffer sb = new StringBuffer();
		sb.append("TRUNCATE ");
		sb.append(tableMapping.getKeyspaceName());
		sb.append('.');
		sb.append(tableMapping.getTableName());
		execute(sb.toString(), cl);
	}

	protected com.datastax.driver.core.ResultSet execute(String cql, ConsistencyLevel cl) throws CassandraDAOException {
		return execute(cql, null, cl);
	}
	protected com.datastax.driver.core.ResultSet execute(String cql, Object[] values, ConsistencyLevel cl) throws CassandraDAOException {
		logger.debug("Executing CQL: {}", cql);
		PreparedStatement stmt;
		try {
			stmt = preparedStatementCache.getPreparedStatement(cql);
		} catch(Exception e) {
			throw new CassandraDAOException(String.format("Exception getting prepared statement for CQL \"%s\"", cql), e);
		}
		
		BoundStatement bstmt;
		try {
			bstmt = values == null ? stmt.bind() : stmt.bind(values);
		} catch(Exception e) {
			throw new CassandraDAOException(String.format("Exception binding variables to statement for CQL \"%s\"", cql), e);
		}
		
		bstmt.setConsistencyLevel(cl);
		
		try {
			return session.execute(bstmt);
		} catch(Exception e) {
			throw new CassandraDAOException(String.format("Exception executing statement for CQL \"%s\"", cql), e);
		}
	}
}
