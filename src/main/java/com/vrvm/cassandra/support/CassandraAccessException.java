package com.vrvm.cassandra.support;

public class CassandraAccessException extends Exception {

    private static final long serialVersionUID = 2125188546760960105L;

    private static final String MSG = "There was a problem accessing Cassandra: ";
    
    public CassandraAccessException() {
        super(MSG);
    }
    
    public CassandraAccessException(String msg) {
        super(MSG + msg);
    }
    
    public CassandraAccessException(String msg, Throwable t) {
        super(MSG + msg, t);
    }

}
