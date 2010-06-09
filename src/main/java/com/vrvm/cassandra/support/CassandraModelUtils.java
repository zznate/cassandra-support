package com.vrvm.cassandra.support;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;

import java.util.ArrayList;
import java.util.List;


public class CassandraModelUtils {
    
    public static List<byte[]> convertColumnNames(List<String> names) {
        List<byte[]> byteNames = new ArrayList<byte[]>(names.size());
        for (String name : names) {
            byteNames.add(bytes(name));
        }
        return byteNames;
    }
    

}
