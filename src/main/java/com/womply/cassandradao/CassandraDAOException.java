package com.womply.cassandradao;

public class CassandraDAOException extends Exception {
	private static final long serialVersionUID = 1L;

	public CassandraDAOException(String msg) {
		this(msg, null);
	}
	
	public CassandraDAOException(Throwable e) {
		this(null, e);
	}
	
	public CassandraDAOException(String msg, Throwable e) {
		super(msg, e);
	}

}
