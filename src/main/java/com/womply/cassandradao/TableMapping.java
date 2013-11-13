package com.womply.cassandradao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.womply.cassandradao.annotations.Column;
import com.womply.cassandradao.annotations.Table;

public class TableMapping<T extends Model> {
	private Class<T> clazz;
	private String keyspaceName;
	private String tableName;
	private List<ColumnMapping> primaryKeys;
	private List<ColumnMapping> partitionKeys;
	private Map<String, ColumnMapping> columnMappings;
	
	public TableMapping(Class<T> modelClass, String keyspaceName, String tableName, Cluster cluster) throws CassandraDAOException {
		this.clazz = modelClass;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;

		if(this.tableName == null) {
			Table table = (Table) modelClass.getAnnotation(Table.class);
			if(table != null && !"".equals(table.name()))
				this.tableName = table.name();
			else
				this.tableName = StringUtils.camelcaseToUnderscores(modelClass.getSimpleName()).toLowerCase() + "s";
		}
		
		KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
		if(keyspaceMetadata == null)
			throw new CassandraDAOException(String.format("Model class '%s' is bound to keyspace '%s' but the keyspace doesn't exist", modelClass.getName(), keyspaceName));
		TableMetadata tableMetadata = keyspaceMetadata.getTable(this.tableName);
		if(tableMetadata == null)
			throw new CassandraDAOException(String.format("Model class '%s' is bound to table '%s.%s' but the table doesn't exist", modelClass.getName(), keyspaceName, this.tableName));

		mapColumns(tableMetadata);
	}
	
	private void mapColumns(TableMetadata tableMetadata) throws CassandraDAOException {
		Map<String, ColumnMapping> columnMappings = new HashMap<String, ColumnMapping>();
		
		// Get defaults based on camel case field name to underscore column name
		for(ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
			String postfix = StringUtils.underscoresToCamelcase(columnMetadata.getName());
			ColumnMapping columnMapping = ColumnMapping.map(clazz, columnMetadata.getName(), postfix);
			if(columnMapping != null)
				columnMappings.put(columnMapping.getName(), columnMapping);
		}
		
		// Override the defaults with annotated fields
		for(Field field : clazz.getDeclaredFields()) {
			Column columnAnnotation = field.getAnnotation(Column.class);
			if(columnAnnotation != null && !StringUtils.isEmpty(columnAnnotation.name())) {
				String postfix = StringUtils.capitalize(field.getName());
				ColumnMapping columnMapping = ColumnMapping.map(clazz, columnAnnotation.name(), postfix);
				if(columnMapping != null)
					columnMappings.put(columnMapping.getName(), columnMapping);
			}
		}
		
		this.columnMappings = columnMappings;
		
		this.primaryKeys = new ArrayList<>();
		this.partitionKeys = new ArrayList<>();
		for(ColumnMetadata columnMeta : tableMetadata.getPrimaryKey()) {
			ColumnMapping columnMapping = columnMappings.get(columnMeta.getName());
			if(columnMapping == null)
				throw new CassandraDAOException(String.format("No column mapping found for primary key %s.%s found in model %s", tableName, columnMeta.getName(), clazz.getName()));
			primaryKeys.add(columnMapping);
			
			if(tableMetadata.getPartitionKey().contains(columnMeta))
				partitionKeys.add(columnMapping);
		}
		if(this.primaryKeys.isEmpty())
			throw new CassandraDAOException(String.format("No primary keys found for table %s.%s", keyspaceName, tableName));
		if(this.partitionKeys.isEmpty())
			throw new CassandraDAOException(String.format("No partition keys found for table %s.%s", keyspaceName, tableName));
	}
	
	protected T marshall(Row row) throws CassandraDAOException {
		T instance = newInstance();
		instance.marshall(this, row);
		return instance;
	}
	
	protected T newInstance() throws CassandraDAOException {
		T instance;
		try {
			instance = clazz.newInstance();
		} catch(Exception e) {
			throw new CassandraDAOException(e);
		}
		return instance;
	}

	public Class<T> getClazz() {
		return clazz;
	}
	public String getKeyspaceName() {
		return keyspaceName;
	}
	public String getTableName() {
		return tableName;
	}
	public Map<String, ColumnMapping> getColumnMappings() {
		return columnMappings;
	}
	public List<ColumnMapping> getPrimaryKeys() {
		return primaryKeys;
	}
	public List<ColumnMapping> getPartitionKeys() {
		return partitionKeys;
	}
}
