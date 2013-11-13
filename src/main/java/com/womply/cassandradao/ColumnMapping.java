package com.womply.cassandradao;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

public class ColumnMapping {
	private String name;
	private Method getter;
	private Method setter;
	private Class<?> setterParamClass;
	private Class<?> setterParamGenericsClass;
	
	@SuppressWarnings("rawtypes")
	public static ColumnMapping map(Class<?> clazz, String columnName, String postfix) {
		String getterName = "get" + postfix;
		String setterName = "set" + postfix;
		Method getter = null;
		Method setter = null;
		Class<?> setterParamType = null;
		Class<?> setterParamGenericsClass = null;
		for(Method method : clazz.getMethods()) {
			if(getterName.equals(method.getName()) && method.getParameterTypes().length == 0) {
				getter = method;
			} else if(setterName.equals(method.getName()) && method.getParameterTypes().length == 1) {
				setter = method;
				setterParamType = setter.getParameterTypes()[0];
				if(setterParamType == Set.class) {
					Type[] types = setter.getGenericParameterTypes();
					if(types.length > 0 && types[0] instanceof ParameterizedType) {
						ParameterizedType paramType = (ParameterizedType) types[0];
						Type[] genericsTypes = paramType.getActualTypeArguments();
						if(genericsTypes.length > 0)
							setterParamGenericsClass = (Class) genericsTypes[0];
					}
				}
			}
		}
		
		return getter != null || setter != null ? new ColumnMapping(columnName, getter, setter, setterParamType, setterParamGenericsClass) : null;
	}
	
	private ColumnMapping(String name, Method getter, Method setter, Class<?> setterParamClass, Class<?> setterParamGenericsClass) {
		this.name = name;
		this.getter = getter;
		this.setter = setter;
		this.setterParamClass = setterParamClass;
		this.setterParamGenericsClass = setterParamGenericsClass;
	}
	
	protected void invokeSetter(Model model, Object obj) throws CassandraDAOException {
		if(setter != null)
			try {
				obj = setter.invoke(model, new Object[] { obj });
			} catch (Exception e) {
				throw new CassandraDAOException(e);
			}
	}

	protected Object invokeGetter(Model model) throws CassandraDAOException {
		Object obj = null;
		if(getter != null)
			try {
				obj = getter.invoke(model, new Object[] { });
			} catch (Exception e) {
				throw new CassandraDAOException(e);
			}
		return obj;
	}

	public String getName() {
		return name;
	}

	public Method getGetter() {
		return getter;
	}

	public Method getSetter() {
		return setter;
	}

	public Class<?> getSetterParamClass() {
		return setterParamClass;
	}

	public Class<?> getSetterParamGenericsClass() {
		return setterParamGenericsClass;
	}
}
