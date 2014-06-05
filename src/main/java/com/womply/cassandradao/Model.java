package com.womply.cassandradao;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

public abstract class Model {
	private Map<String, ColumnState> columnStatesByName = new HashMap<String, Model.ColumnState>();
	private TableMapping<? extends Model> tableMapping;
	
	protected void marshall(TableMapping<? extends Model> modelMapping, Row row) throws CassandraDAOException {
		this.columnStatesByName.clear();;
		this.tableMapping = modelMapping;
		
		int index = -1;
		for(ColumnDefinitions.Definition colDef : row.getColumnDefinitions()) {
			index++;
			ColumnMapping colMapping = modelMapping.getColumnMappings().get(colDef.getName());
			if(colMapping == null || colMapping.getSetter() == null)
				continue;

			Object value;
			if(colMapping.getSetterParamClass() == String.class)
				value = row.getString(index);
			else if(colMapping.getSetterParamClass() == int.class || colMapping.getSetterParamClass() == Integer.class)
				value = row.getInt(index);
			else if(colMapping.getSetterParamClass() == long.class || colMapping.getSetterParamClass() == Long.class)
				value = row.getLong(index);
			else if(colMapping.getSetterParamClass() == float.class || colMapping.getSetterParamClass() == Float.class)
				value = row.getFloat(index);
			else if(colMapping.getSetterParamClass() == double.class || colMapping.getSetterParamClass() == Double.class)
				value = row.getDouble(index);
			else if(colMapping.getSetterParamClass() == BigDecimal.class)
				value = row.getDecimal(index);
			else if(colMapping.getSetterParamClass() == Date.class)
				value = row.getDate(index);
			else if(colMapping.getSetterParamClass() == UUID.class)
				value = row.getUUID(index);
			else if(colMapping.getSetterParamClass() == Boolean.class)
				value = row.getBool(index);
			else if(colMapping.getSetterParamClass() == Set.class)
				value = row.getSet(index, colMapping.getSetterParamGenericsClass());
			else if(colMapping.getSetterParamClass() == List.class)
				value = row.getList(index, colMapping.getSetterParamGenericsClass());
			else
				throw new CassandraDAOException("Could not find type!");

			columnStatesByName.put(colMapping.getName(), new ColumnState(value));
			try {
				colMapping.invokeSetter(this, value);
			} catch(Exception e) {
				throw new CassandraDAOException(e);
			}
		}
	}
	
	protected void updateColumnStates() throws CassandraDAOException {
		columnStatesByName.clear();
		for(ColumnMapping columnMapping : tableMapping.getColumnMappings().values()) {
			Object value = columnMapping.invokeGetter(this);
			if(value != null)
				columnStatesByName.put(columnMapping.getName(), new ColumnState(value));
		}
	}
	
	public void resetColumnStates() {
		columnStatesByName.clear();
	}
	
	protected Map<ColumnMapping, Object> getPrimaryKeyValuesByColumnMapping(TableMapping<? extends Model> mapping) throws CassandraDAOException {
		Map<ColumnMapping, Object> primaryKeyValues = new HashMap<>();
		for(ColumnMapping columnMapping : mapping.getPrimaryKeys()) {
			primaryKeyValues.put(columnMapping, columnMapping.invokeGetter(this));
		}
		return primaryKeyValues;
	}
	
	protected Map<ColumnMapping, Object> getChangedValuesByColumnMappings(TableMapping<? extends Model> mapping) throws CassandraDAOException {
		if(tableMapping == null)
			this.tableMapping = mapping;
		
		Map<ColumnMapping, Object> changedValuesByColumnMappings = new HashMap<ColumnMapping, Object>();
		for(ColumnMapping columnMapping : tableMapping.getColumnMappings().values()) {
			Object value = columnMapping.invokeGetter(this);
			ColumnState columnState = columnStatesByName.get(columnMapping.getName());
			boolean changed = false;
			if(columnState == null) {
				if(value != null)
					changed = true;
			} else {
				if(columnState.marshalledValue != null && value != null) {
					changed = !value.equals(columnState.marshalledValue);
				} else {
					changed = (columnState.marshalledValue != null && value == null) ||
							(columnState.marshalledValue == null && value != null);
				}
			}
			
			if(changed)
				changedValuesByColumnMappings.put(columnMapping, value);
		}
		
		return changedValuesByColumnMappings;
	}
	
	private class ColumnState {
		private ColumnState(Object marshalledValue) {
			this.marshalledValue = marshalledValue;
		}
		
		private Object marshalledValue;
	}
}
