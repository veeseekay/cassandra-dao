package com.womply.cassandradao;

public class CassandraDAORuntimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public CassandraDAORuntimeException(Exception e) {
		super(e);
	}

}
