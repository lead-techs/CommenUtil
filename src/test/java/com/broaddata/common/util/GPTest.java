/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.broaddata.common.util;

import com.broaddata.common.util.CAUtil;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.ServiceGuardConnector;
import com.broaddata.common.util.CommonServiceConnector;
import com.broaddata.common.util.Util;
import com.broaddata.common.util.FileUtil;
import com.broaddata.common.model.enumeration.ComputingNodeService;
import com.broaddata.common.model.enumeration.JobTimeType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.TimeUnit;
import com.broaddata.common.model.organization.Content;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.Metadata;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.model.pojo.DataWorkerAdminData;
import com.broaddata.common.processor.Processor;

import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.commonservice.CommonService.Client;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.thrift.serviceguard.ServiceGuard;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Query;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.dom4j.Document;
import org.elasticsearch.action.update.UpdateResponse;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fangf
 */
public class GPTest {
    
    public GPTest() {
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
    public void readDataFromGP()
    {                
        Connection dbConn = null;
        int organizationId = 100;
        int repositoryId = 1;
        int datasourceConnectionId = 23;
        int dataobjectTypeId = 2290;
        
        try
        {
            Util.commonServiceIPs="127.0.0.1";
            EntityManager em = Util.getEntityManagerFactory(organizationId).createEntityManager();
            EntityManager platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            
            DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
           
            DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class,datasourceConnectionId);
 
            String schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
            if ( schema.equals("%") )
                schema = "";
            
            //DatabaseType databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
            dbConn = JDBCUtil.getJdbcConnection(datasourceConnection);
                                                              
         /*  String tableName = Util.getDataobjectTableName(em,platformEm, organizationId, dataobjectTypeId,dataobjectType.getName(),repositoryId);
           
            List<String> primaryKeyFieldNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());
            
            String primaryKeyFieldNamesStr = "";
            List<String> tableColumnNames = new ArrayList<>();
            for(String primaryKeyFieldName : primaryKeyFieldNames)
            {
                String tableColumnName = Tool.changeTableColumnName(dataobjectType.getName(),primaryKeyFieldName,true);
                tableColumnNames.add(tableColumnName);
                primaryKeyFieldNamesStr+= tableColumnName+",";
            }
            
            if ( !primaryKeyFieldNamesStr.isEmpty() )
                primaryKeyFieldNamesStr = primaryKeyFieldNamesStr.substring(0, primaryKeyFieldNamesStr.length()-1);
            */
         
            int pageSize = 10000000;
            int from = 0;
            int i = 1;
            int k = 0;
                
            while(true)
            {
                k++;
                Date startTime = new Date();
                
                String querySQL = String.format("select seq_id from sale_order_hen order by seq_id limit %d offset %d",pageSize,from);
                                
                PreparedStatement statement = dbConn.prepareStatement(querySQL);
                statement.setFetchSize(500);
 
                ResultSet  resultSet = JDBCUtil.executeQuery(statement);
                
                System.out.println("k="+k+", took "+(new Date().getTime()-startTime.getTime())+" ms, from="+from+" pageSize="+pageSize);
                
                if ( resultSet.next() == false )
                {
                    System.out.printf("1111111111111 resultSet.next() is false!  exit!");
                    break;
                }
                                 
                from += pageSize;
             
                do
                {  
                    long seqId = resultSet.getLong("seq_id");
                    //System.out.println("i="+i+",seq_id="+seqId);
                    i++;
                }
                while( resultSet.next() );
 
                JDBCUtil.close(resultSet);
                JDBCUtil.close(statement);
            }
        }
        catch(Throwable e)
        {
            String error = " generateMetricsCalculationJob() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e);
            System.out.printf(error);
        }
        finally
        {
            JDBCUtil.close(dbConn);
        }
 
    }
    
}
