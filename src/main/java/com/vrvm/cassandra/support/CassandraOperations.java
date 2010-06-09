package com.vrvm.cassandra.support;

import java.util.List;
import java.util.Map;


public interface CassandraOperations {
    
    <T> T execute(CassandraCallback<T> callback) throws CassandraAccessException;
    
    String get(String key, String columnName) throws CassandraAccessException;
    
    <T> List<T> get(String key, List<String> columnNames, ColumnExtractor<T> columnExtractor) throws CassandraAccessException;
    
    void insert(String key, String columnName, String value) throws CassandraAccessException;
    
    void insert(String key, Map<String, String> columnMap) throws CassandraAccessException;
    
    void delete(String key, String columnName) throws CassandraAccessException;
    
    void delete(String key) throws CassandraAccessException;
    
    void batchMutate(BatchMutationHelper bhm) throws CassandraAccessException;
    
}
