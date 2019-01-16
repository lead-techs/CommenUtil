/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.organization.DatasourceConnection;
import static com.broaddata.common.util.JDBCUtil.getJdbcConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author edf
 */
public class JDBCUtilTest {
    
    public JDBCUtilTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After 
    public void tearDown() {
    }
             
    @Ignore 
    @Test
    public void test111() throws Exception
    {
        DatabaseType databaseType = DatabaseType.ORACLE_8i;
        
        URL jarUrl = new URL("jar","","file:D:/Fang/jdbc14-14.jar!/"); 
                
        URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl}); 
        Class driverClass = classLoader.loadClass(databaseType.getDriver());
        Driver driver = (Driver) driverClass.newInstance();
        DriverManager.registerDriver(driver);
         
        //DriverManager.deregisterDriver(driver);
        
        int k=0;
        
         jarUrl = new URL("jar","","file:D:/Fang/jdbc7-7.jar!/"); 
                
         classLoader = new URLClassLoader(new URL[]{jarUrl}); 
        driverClass = classLoader.loadClass(databaseType.getDriver());
         driver = (Driver) driverClass.newInstance();
        DriverManager.registerDriver(driver);
         
        DriverManager.deregisterDriver(driver);
        
        k=0;
    }
                
  @Ignore
    @Test
    public void testAAA() throws Exception
    {
        Util.commonServiceIPs="127.0.0.1";
        EntityManager em = Util.getEntityManagerFactory(100).createEntityManager();
            
        DatasourceConnection dc = em.find(DatasourceConnection.class, 40); //14
        
        Connection dbConn = JDBCUtil.getJdbcConnection(dc);
              
        String schema = "%";
        String tableName = "aaa";
        
        boolean exist = JDBCUtil.isTableExists(dbConn, schema, tableName);
                              
        JDBCUtil.libFilePath = "D:\\Fang\\";
        
        List<String> list1 = JDBCUtil.getDatabaseTableOrViewList(dc, "TABLE");
        
        dc = em.find(DatasourceConnection.class, 25); //14
        
       // JDBCUtil.libFilePath = Tool.getLibFilePath(this);
        
        list1 = JDBCUtil.getDatabaseTableOrViewList(dc, "TABLE");
        
        dc = em.find(DatasourceConnection.class, 27); //14
        
       // JDBCUtil.libFilePath = Tool.getLibFilePath(this);
        
        list1 = JDBCUtil.getDatabaseTableOrViewList(dc, "TABLE");
        
        int i=0;
        //List<String> list = JDBCUtil.getDatabaseStoredProcedureNameList(dc,"EDF");

    }
    
    /**
     * Test of testJdbcConnection method, of class JDBCUtil.
     */
    @Ignore
    @Test
    public void testSparkSQL() throws Exception 
    {
        try
        {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection con = DriverManager.getConnection(
                            "jdbc:hive2://192.168.1.60:10000/hive?characterEncoding=UTF-8",
                            "spark", "");
            Statement stmt = con.createStatement();
            
            String sql = "show tables";
       
            ResultSet rs=stmt.executeQuery(sql);
            while (rs.next()) {
                    System.out.println(rs.getString(1));
            }
            
           sql = "describe test3";
        
            rs=stmt.executeQuery(sql);
            while (rs.next()) {
                    System.out.println(rs.getString("col_name"));
                    System.out.println(rs.getString("data_type"));
                    System.out.println(rs.getInt("data_type"));
                    
            }
            
           
            
           sql = "insert into table test3 select * from (select 555, 'key555 ') a";
           //String sql = "update test3 set value='new3333' where key=333333";// select * from (select 333333, 'key 333333333 ') a"; 
           stmt.executeQuery(sql);
           
           sql = "insert overwrite table test3 select * from (select 5555, 'key555--------------- ') a";
           stmt.executeQuery(sql);
            
            rs=stmt.executeQuery("select key,value from test3");
            while (rs.next()) {
                    System.out.println(rs.getInt("key")+","+rs.getString("value"));
            }
            //conn1.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate();
            //stmt.executeQuery("create table test2 (key int, value string)");
            // load data into table
            /*for (int i = 0; i < 100; i++) {
                    String sql="insert into table test2 select * from (select "
                                    + (i + 1) + ", 'string for " + (i + 1) + "') a";
                    System.out.println(sql);
                    stmt.executeQuery(sql);
            }*/
            rs=stmt.executeQuery("select key,value from test3");
            while (rs.next()) {
                    System.out.println(rs.getInt("key")+","+rs.getString("value"));
            }
        } catch (Exception e) {
                e.printStackTrace();
        }
    }
             
 @Ignore
    @Test
    public void testTestJdbcConnection() throws Exception 
    {
        //jdbc:mysql://localhost:3306/testdb?zeroDateTimeBehavior=convertToNull
        System.out.println("testJdbcConnection");
        int databaseTypeId = 1;
        String IPAddress = "localhost";
        String port = "3306";
        String databaseName = "testdb";
        String schema = "";
        String user = "root";
        String password = "root";
        String expResult = "database_connection_succeeded";
        String result = JDBCUtil.testJdbcConnection(databaseTypeId, IPAddress, port, databaseName, schema, user, password);
        
        String tableName = "core_bdfmhqab";
        Connection conn = JDBCUtil.getJdbcConnection(databaseTypeId, IPAddress, port, databaseName, schema, user, password);
 
            
            DatabaseMetaData databaseMetaData = conn.getMetaData(); 
            ResultSet tableRet =databaseMetaData.getTables(null,schema,tableName,new String[]{"TABLE"});
            tableRet.next();
            String tableDescription = tableRet.getString("REMARKS");    
        
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getConnection method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testGetConnection() throws Exception {
        System.out.println("getConnection");
        String driver = "";
        String dbURL = "";
        String user = "";
        String password = "";
        Connection expResult = null;
        Connection result = JDBCUtil.getConnection(driver, dbURL, user, password);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of executeUpdate method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testExecuteUpdate() {
        System.out.println("executeUpdate");
        Statement statement = null;
        String sql = "";
        boolean expResult = false;
        boolean result = JDBCUtil.executeUpdate(statement, sql);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of executeQuery method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testExecuteQuery() throws Exception {
        System.out.println("executeQuery");
        PreparedStatement statement = null;
        ResultSet expResult = null;
        ResultSet result = JDBCUtil.executeQuery(statement);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testClose_ResultSet() {
        System.out.println("close");
        ResultSet resultSet = null;
        JDBCUtil.close(resultSet);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testClose_Statement() {
        System.out.println("close");
        Statement statement = null;
        JDBCUtil.close(statement);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class JDBCUtil.
     */
     @Ignore
    @Test
    public void testClose_Connection() {
        System.out.println("close");
        Connection conn = null;
        JDBCUtil.close(conn);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
