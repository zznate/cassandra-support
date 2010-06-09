package com.vrvm.cassandra.support;

import me.prettyprint.cassandra.service.Keyspace;


public interface CassandraCallback<T> {

    T doInCassandra(final Keyspace ks) throws Exception;
    
}
