package com.vrvm.cassandra.support;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.TimestampResolution;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses {@link CassandraCallback} to encapsulate common actions on Apache Cassandra
 * via the Hector API. In design pattern parlance, this would be a command pattern
 * in which this class is the invoker, {@link CassandraCallback} is the command, and
 * the underlying KeyspaceImpl would be the receiver. 
 * 
 * @author Nate McCall <nate@vervewireless.com>
 */
public class CassandraTemplate implements CassandraOperations {

    private Logger log = LoggerFactory.getLogger(CassandraTemplate.class);
    
    private String columnFamilyName;
    private String keyspace;
    private CassandraClientPool cassandraClientPool;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;

    
    @Override
    public void batchMutate(BatchMutationHelper bhm) throws CassandraAccessException {
        execute(bhm);
    }

    @Override
    public void delete(final String key, final String columnName) throws CassandraAccessException {
        execute(new CassandraCallback<Void>() {
            public Void doInCassandra(final Keyspace ks) throws Exception {
                ks.remove(key, createColumnPath(columnName));
                return null;
            }
        });
    }

    @Override
    public void delete(final String key) throws CassandraAccessException {
        execute(new CassandraCallback<Void>() {
            public Void doInCassandra(final Keyspace ks) throws Exception {
                ks.remove(key, createColumnPath(null));
                return null;
            }
        });
    }


    @Override
    public String get(final String key, final String columnName) throws CassandraAccessException {
        return execute(new CassandraCallback<String>(){
            public String doInCassandra(final Keyspace ks) throws Exception {                
                try {
                    return string(ks.getColumn(key, createColumnPath(columnName)).getValue());
                } catch (NotFoundException e) {
                    return null;
                }
            }
        }); 
    }

    @Override
    public <T> List<T> get(final String key, 
            final List<String> columnNames, 
            final ColumnExtractor<T> columnExtractor) throws CassandraAccessException {
        return execute(new CassandraCallback<List<T>>(){
            public List<T> doInCassandra(final Keyspace ks) throws Exception {                
                try {
                    ColumnParent clp = new ColumnParent(columnFamilyName);
                    SlicePredicate sp = new SlicePredicate();
                    sp.setColumn_names(CassandraModelUtils.convertColumnNames(columnNames));
                    List<Column> columns = ks.getSlice(key, clp, sp);
                    if ( log.isDebugEnabled() ) {
                        log.debug("found columns {}", columns);
                    }
                    List<T> vals = new ArrayList<T>(columns.size());
                    for (Column col : columns) {
                        vals.add(columnExtractor.extract(col.name, col.value, col.timestamp));
                    }
                    return vals;
                } catch (NotFoundException e) {
                    if ( log.isDebugEnabled()) {
                        log.debug("Not found: ", e);
                    }
                    return null;
                }
            }
        });
    }

    @Override
    public void insert(final String key, final String columnName, final String value) throws CassandraAccessException {
        execute(new CassandraCallback<Void>(){
            public Void doInCassandra(final Keyspace ks) throws Exception {
                ks.insert(key, createColumnPath(columnName), bytes(value));
                return null;
            }
        });
    }

    @Override
    public void insert(String key, Map<String, String> columnMap) throws CassandraAccessException {
        BatchMutationHelper batchMutationHelper = new BatchMutationHelper(columnFamilyName, buildCommonTimestamp());
        for (String colName : columnMap.keySet()) {
            batchMutationHelper.addInsertion(key, colName, columnMap.get(colName));  
        }
        execute(batchMutationHelper);
    }
    
    @Override
    public <T> T execute(CassandraCallback<T> cassandraCallback) throws CassandraAccessException {
        CassandraClient c = null;
        Keyspace ks = null; 
        try {
            c = cassandraClientPool.borrowClient();
            ks = c.getKeyspace(keyspace, consistencyLevel);
            return cassandraCallback.doInCassandra(ks);     
        } catch (Exception e) {
            throw new CassandraAccessException("Could not execute operation", e);        
        } finally {
            try {
                cassandraClientPool.releaseClient(ks.getClient());
            } catch (Exception e) {
                log.error("Could not release client",e);
            }
        }             
    }

    protected ColumnPath createColumnPath(String columnName) {
        ColumnPath columnPath = new ColumnPath(columnFamilyName);
        if ( columnName != null ) {
            columnPath.setColumn(bytes(columnName));
        }
        return columnPath;
    }
    
    public long buildCommonTimestamp() {
        return TimestampResolution.MICROSECONDS.createTimestamp();
    }
    
    /**
     * Build a BatchMutationHelper with this columnFamilyName and the timestamp of now
     * @return {@link BatchMutationHelper} for this template's columnFamily with a 
     * micro-second timestamp of now
     */
    public BatchMutationHelper getBatchMutationHelper() {
        return new BatchMutationHelper(columnFamilyName, buildCommonTimestamp());
    }
    
    /**
     * Builds a BatchMutationHelper with the user-specified timestamp.
     * WARNING: timestamps must be in MICROseconds or you could get unexpected behaviour
     * @param timestamp
     * @return {@link BatchMutationHelper}
     */
    public BatchMutationHelper getBatchMutationHelper(long timestamp) {
        return new BatchMutationHelper(columnFamilyName, timestamp);
    }
    /**
     * The ColumnFamily on which this template will operate
     * @param columnFamily
     */
    public void setColumnFamilyName(String columnFamily) {
        this.columnFamilyName = columnFamily;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
    
    public void setCassandraClientPool(CassandraClientPool cassandraClientPool) {
        this.cassandraClientPool = cassandraClientPool;
    }

    public void setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = ConsistencyLevel.valueOf(consistencyLevel);
    }
    
    
    
}
