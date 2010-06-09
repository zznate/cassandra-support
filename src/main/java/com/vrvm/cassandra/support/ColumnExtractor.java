package com.vrvm.cassandra.support;

/**
 * Used on get* API methods that return multiple columns 
 *
 * @author zznate
 */
public interface ColumnExtractor<T> {

    /**
     * Extract the columnName and columnValue into an arbitrary object
     * @param columnName
     * @param columnValue
     * @return
     */
    T extract(byte[] columnName, byte[] columnValue, long timestamp);
    
}
