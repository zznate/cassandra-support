package com.vrvm.cassandra.support;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.Keyspace;

public class BatchMutationHelper implements CassandraCallback<Void> {
    
    private Logger log = LoggerFactory.getLogger(BatchMutationHelper.class);
    
    protected final long timestamp;
    protected final String columnFamilyName;
    private final BatchMutation batchMutation;
    
    BatchMutationHelper(String columnFamilyName, long timestamp) {
        this.columnFamilyName = columnFamilyName;
        this.timestamp = timestamp;
        batchMutation = new BatchMutation();
    }
    
    public void addInsertion(String key, String columnName, String columnValue) {
        if ( log.isDebugEnabled()) {
            log.debug("adding insertion key {} columnName {} value {}", new Object[]{key, columnName, columnValue});
        }
        batchMutation.addInsertion(key, Arrays.asList(columnFamilyName), 
                new Column(bytes(columnName), bytes(columnValue), timestamp));          
    }
    
    public void addDeletion(String key, String columnName) {
        addDeletion(key, Arrays.asList(columnName));
    }
    
    public void addDeletion(String key, List<String> columnNames) {
        Deletion deletion = new Deletion(timestamp);
        SlicePredicate slicePredicate = new SlicePredicate();
        for (String columnName : columnNames) {
            slicePredicate.addToColumn_names(bytes(columnName));    
        }        
        deletion.setPredicate(slicePredicate);
        batchMutation.addDeletion(key, Arrays.asList(columnFamilyName), deletion);
    }
    
    public Void doInCassandra(Keyspace keyspace) throws CassandraAccessException {        
        try {
            keyspace.batchMutate(batchMutation);
        } catch (InvalidRequestException ire) {
            throw new CassandraAccessException("There was a problem with the format of the request", ire);        
        } catch (Exception e) {
            throw new CassandraAccessException("Could not execute batchInsert" + e.getMessage(), e);
        }
        
        return null;        
    }
}
