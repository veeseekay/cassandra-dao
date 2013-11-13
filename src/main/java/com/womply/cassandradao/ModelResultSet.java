package com.womply.cassandradao;

import java.util.Iterator;

import com.datastax.driver.core.Row;

public class ModelResultSet<T extends Model> implements ResultSet<T> {

	private com.datastax.driver.core.ResultSet rs;
	private TableMapping<T> tableMapping;
	private T instance;

	public ModelResultSet(TableMapping<T> modelMapping, com.datastax.driver.core.ResultSet rs) {
		this.tableMapping = modelMapping;
		this.rs = rs;
	}

	@Override
	public void setInstance(T instance) {
		this.instance = instance;
	}

	public Iterator<T> iterator() {
		return new ModelResultSetIterator(rs.iterator());
	}
	
	private class ModelResultSetIterator implements Iterator<T> {
		
		private Iterator<Row> iterator;

		public ModelResultSetIterator(Iterator<Row> iterator) {
			this.iterator = iterator;
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}

		public T next() {
			Row row = iterator.next();
			try {
				T instance = ModelResultSet.this.instance;
				if(instance == null)
					instance = tableMapping.newInstance();
				else
					instance.resetColumnStates();

				instance.marshall(tableMapping, row);
				return instance;
			} catch (CassandraDAOException e) {
				throw new CassandraDAORuntimeException(e);
			}
		}

		public void remove() {
			iterator.remove();
		}
	}
}
