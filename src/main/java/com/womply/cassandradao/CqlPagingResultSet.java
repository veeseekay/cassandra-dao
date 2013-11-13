package com.womply.cassandradao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.datastax.driver.core.ConsistencyLevel;

public class CqlPagingResultSet<T extends Model> implements ResultSet<T> {
	private CassandraDAO<T> cassandraDAO;
	private TableMapping<T> tableMapping;
	private String cql;
	private int maxPageSize;
	private T instance;
	private ConsistencyLevel cl;

	public CqlPagingResultSet(CassandraDAO<T> dao, TableMapping<T> modelMapping, String cql, Integer maxPageSize, ConsistencyLevel cl) throws CassandraDAOException {
		this.cassandraDAO = dao;
		this.tableMapping = modelMapping;
		this.cql = cql;
		this.maxPageSize = maxPageSize == null ? 10000 : maxPageSize;
		this.cl = cl;
	}

	@Override
	public void setInstance(T instance) {
		this.instance = instance;
	}

	@Override
	public Iterator<T> iterator() {
		return new PagingResultSetIterator();
	}
	
	private class PagingResultSetIterator implements Iterator<T> {
		private T currentItem;
		private Iterator<T> currentResultSet;
		private int currentResultSetCount;
		private Object[] lastPrimaryKeyValues = new Object[tableMapping.getPrimaryKeys().size()];

		@Override
		public boolean hasNext() {
			if(currentItem != null)
				return true;
			
			currentItem = readNext();
			
			return currentItem != null;
		}

		@Override
		public T next() {
			if(currentItem == null)
				currentItem = readNext();
			if(currentItem == null)
				throw new NoSuchElementException();
			
			T item = currentItem;
			currentItem = null;
			return item;
		}
		
		private T readNext() {
			try {
				while(currentResultSet == null || !currentResultSet.hasNext()) {
					ResultSet<T> rs = null;
					if(currentResultSet == null) {
						rs = new ModelResultSet<T>(tableMapping, cassandraDAO.execute(String.format("%s LIMIT %d", cql, maxPageSize), cl));
						rs.setInstance(CqlPagingResultSet.this.instance);
					} else if(!currentResultSet.hasNext()) {
						int lastNonNullPrimaryKeyValueIndex = 0;
						for(int i = 0; i < lastPrimaryKeyValues.length; i++) {
							if(lastPrimaryKeyValues[i] == null)
								break;
							else
								lastNonNullPrimaryKeyValueIndex = i;
						}
						if(currentResultSetCount != maxPageSize) {
							lastPrimaryKeyValues[lastNonNullPrimaryKeyValueIndex] = null;
							lastNonNullPrimaryKeyValueIndex--;
						}
						if(lastNonNullPrimaryKeyValueIndex + 1 < tableMapping.getPartitionKeys().size())
							return null;
						
						StringBuffer lastPrimaryKeyWhere = new StringBuffer();
						boolean partitionKeyOnly = lastNonNullPrimaryKeyValueIndex + 1 == tableMapping.getPartitionKeys().size();
						if(partitionKeyOnly) {
							StringBuffer sb = new StringBuffer("token(");
							lastPrimaryKeyWhere.append("token(");
							for(int i = 0; i <= lastNonNullPrimaryKeyValueIndex; i++) {
								ColumnMapping columnMapping = tableMapping.getPrimaryKeys().get(i);
								lastPrimaryKeyWhere.append(columnMapping.getName());
								sb.append("?");
								
								boolean last = lastNonNullPrimaryKeyValueIndex == i;
								if(!last) {
									lastPrimaryKeyWhere.append(", ");
									sb.append(", ");
								}
							}
							lastPrimaryKeyWhere.append(") > ");
							lastPrimaryKeyWhere.append(sb);
							lastPrimaryKeyWhere.append(")");
						} else {
							for(int i = 0; i <= lastNonNullPrimaryKeyValueIndex; i++) {
								ColumnMapping columnMapping = tableMapping.getPrimaryKeys().get(i);
								boolean last = lastNonNullPrimaryKeyValueIndex == i;
								lastPrimaryKeyWhere.append(columnMapping.getName());
								lastPrimaryKeyWhere.append(last ? " > ?" : " = ?");
								if(!last)
									lastPrimaryKeyWhere.append(" AND ");
							}
						}
						
						List<Object> lastPrimaryKeyParams = new ArrayList<>();
						for(int i = 0; i <= lastNonNullPrimaryKeyValueIndex; i++) {
							lastPrimaryKeyParams.add(lastPrimaryKeyValues[i]);
						}
						
						String pageCql = String.format("%s WHERE %s LIMIT %d", cql, lastPrimaryKeyWhere, maxPageSize);
						rs = new ModelResultSet<>(tableMapping, cassandraDAO.execute(pageCql, lastPrimaryKeyParams.toArray(), cl));
						currentResultSetCount = 0;
					}
					
					currentResultSet = rs.iterator();
				}
				
				T next = null;
				if(currentResultSet.hasNext()) {
					next = currentResultSet.next();
					currentResultSetCount++;

					if(!currentResultSet.hasNext()) {
						for(int i = 0; i < tableMapping.getPrimaryKeys().size(); i++) {
							ColumnMapping primaryKey = tableMapping.getPrimaryKeys().get(i);
							lastPrimaryKeyValues[i] = primaryKey.invokeGetter(next);
						}
					}
				}
				
				return next;
			} catch(CassandraDAOException e) {
				throw new CassandraDAORuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
