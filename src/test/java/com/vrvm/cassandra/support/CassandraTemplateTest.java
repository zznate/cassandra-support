package com.vrvm.cassandra.support;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import me.prettyprint.cassandra.model.Column;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="/cassandra-context-test.xml")
public class CassandraTemplateTest {

    private static EmbeddedServerHelper embeddedServerHelper;
    
    @Resource
    protected CassandraTemplate cassandraTemplate;
    
    @BeforeClass
    public static void setup() throws Exception {
        embeddedServerHelper = new EmbeddedServerHelper();
        embeddedServerHelper.setup();
    }
    
    @AfterClass
    public static void teardown() throws Exception {
        embeddedServerHelper.teardown();
    }
    
    @Test
    public void test_insert_retrieve_delete_single() throws Exception {
        cassandraTemplate.insert(KEY_APPID_1, COLUMN_ONE, VALUE_ONE);
        String val = cassandraTemplate.get(KEY_APPID_1, COLUMN_ONE);
        assertEquals(VALUE_ONE,val);
        cassandraTemplate.delete(KEY_APPID_1,COLUMN_ONE);
        val = cassandraTemplate.get(KEY_APPID_1, COLUMN_ONE);
        assertNull(val);
    }
    
    @Test
    public void test_get_list() throws Exception {
        Map<String, String> columns = generateAndInsertRow(KEY_APPID_1, 10);
        List<Column> cols = cassandraTemplate.get(KEY_APPID_1, new ArrayList<String>(columns.keySet()), new ColumnExtractor<Column>() {
            public Column extract(byte[] columnName,
                    byte[] columnValue,
                    long timestamp) {
                return new Column(columnName, columnValue, timestamp);
            }            
        });
        assertNotNull(cols);
        assertEquals(10, cols.size());
    }
    
    protected Map<String,String> generateAndInsertRow(String rowKey, int columnCount) throws Exception {
        Map<String,String> columns = generateColumn(columnCount);
        // consolidate this loop into generate, perhaps we do this in @before, removal in @after?
        for (String colName : columns.keySet()) {
            cassandraTemplate.insert(rowKey,colName,columns.get(colName));                                         
        }                
        return columns;
    }
    
    protected Map<String, String> generateColumn(int count) {        
        HashMap<String, String> row = new HashMap<String, String>(count);
        for (int i = 0; i < count; i++) {            
            row.put(Integer.toString(i), RandomStringUtils.randomAlphabetic(32));
        }
        return row;
    }

    private static final String KEY_APPID_1 = "appid_1";
    private static final String VALUE_ONE = "value one";
    private static final String COLUMN_ONE = "column_one";

    
}
