package com.womply.cassandradao;

public interface ResultSet<T> extends Iterable<T> {

	public void setInstance(T instance);
}
