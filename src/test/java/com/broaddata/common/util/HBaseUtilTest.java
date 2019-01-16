/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;
 
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass; 
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Ignore;

/**
 *
 * @author fangf
 */
public class HBaseUtilTest {
    
    public HBaseUtilTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
   @Ignore
    @Test
    public void test1() throws Exception
    {
        String hbaseIp = "192.168.1.31";
        String hbasePort = "2181";
        String znodeParent = "/hbase";
        String familiesStr = "edf";
        String tableName = "test_table";
     
        //List<Map<String,Object>> list = HBaseUtil.getAllTableColumnName(hbaseIp,hbasePort,znodeParent,tableName,familiesStr);
        
        String sqlLikeString = "select * from test_table where orginal_date1='original_data_u1001c1001_111111111111111111111111111111111'";
        //SimpleSQLSturecture s = HBaseUtil.convertSQLLikeStringToStructure(sqlLikeString);
        
        HBaseUtil.initConn(hbaseIp, hbasePort, znodeParent,familiesStr); 
        Map<String,Object> ret  = HBaseUtil.getRowDataBySQL(sqlLikeString,0); 
        
        System.out.print("s="+ret);
    }
  
    @Ignore
    @Test
    public void test()
    {                
        String hbaseIp = "192.168.1.31";
        String hbasePort = "2181";
        String znodeParent = "/hbase";
        String familiesStr = "edf";
        String tableName = "test_table";
              
        try
        {
            HBaseUtil.initConn(hbaseIp, hbasePort, znodeParent,familiesStr); 
            
            HBaseUtil.dropTable(tableName);
             
            HBaseUtil.createTable(tableName,familiesStr);
        
            String rowKey1 = "u1001c1001";
            Map<String,String> columnData = new HashMap<>();
            
            columnData.put("original_data1", "original_data_u1001c1001_111111111111111111111111111111111");
            columnData.put("original_data2", "original_data_u1001c1001_111111111111122222222222222222222");
            columnData.put("original_data3", "original_data_u1001c1001_111111111111133333333333333333333");
           
            HBaseUtil.addData(tableName, familiesStr, rowKey1, columnData); 
            
            String rowKey2 = "u1001c1002";
            columnData = new HashMap<>();
            columnData.put("original_data1", "original_data_u1001c1001_22222222222222111111111111111");
            columnData.put("original_data2", "original_data_u1001c1001_22222222222222222222222222222");
            columnData.put("original_data3", "original_data_u1001c1001_22222222222223333333333333333");
            HBaseUtil.addData(tableName, familiesStr, rowKey2, columnData);
            
            Map<String,String> row1 = HBaseUtil.getRowData(tableName, familiesStr, rowKey1);
            
            String column2 = HBaseUtil.getRowOneColumnData(tableName, familiesStr, rowKey1, "original_data11");
 
            List<String> rowKeys = new ArrayList<>();
            rowKeys.add(rowKey1);
            rowKeys.add(rowKey2);
            
            List<Map<String,String>> ret = HBaseUtil.getMutipleRowData(tableName,familiesStr,rowKeys,null);
                        
           /* Map<String,Map<Object,String>> conditions = new HashMap<>();
            
            Map<Object,String> condition = new HashMap<>();
            condition.put(CompareOp.EQUAL, "original_data_u1001c1001_111111111111133333333333333333333");
            conditions.put("original_data3", condition);
            
            String[] columns = new String[]{"original_data3"};
            
            ret = HBaseUtil.getRowDataByConditions(tableName,familiesStr,conditions,Arrays.asList(columns),0,10000,null,null); */
    
            String sqlLikeString = "SELECT * FROM test_table.edf where original_data3='original_data_u1001c1001_111111111111133333333333333333333' ";
             Map<String,Object> ret1 = HBaseUtil.getRowDataBySQL(sqlLikeString,0); 
            
            sqlLikeString = "SELECT original_data1,original_data3 FROM test_table.edf where original_data3='original_data_u1001c1001_111111111111133333333333333333333' ";
            ret1 = HBaseUtil.getRowDataBySQL(sqlLikeString,0); 
      
      
            System.out.print("finish");
        }
        catch(Throwable e)
        {
            String error = " test() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            System.out.printf(error);
        }
        finally
        {
            HBaseUtil.closeConn();
        }
    }
}
